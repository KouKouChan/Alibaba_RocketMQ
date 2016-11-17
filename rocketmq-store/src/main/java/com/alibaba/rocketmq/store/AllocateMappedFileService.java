/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * 预分配MappedFile服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final int WAIT_TIME_OUT = 1000 * 5;
    private ConcurrentHashMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;
    private final DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextPutOK = (this.requestTable.putIfAbsent(nextFilePath, nextReq) == null);
        boolean nextNextPutOK = (this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null);

        if (nextPutOK) {
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }

        if (nextNextPutOK) {
            boolean offerOK = this.requestQueue.offer(nextNextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(WAIT_TIME_OUT, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                }
                this.requestTable.remove(nextFilePath);
                return result.getMappedFile();
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        }
        catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }


    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }


    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJoinTime());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated mapped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }


    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation())
            ;

        log.info(this.getServiceName() + " service end");
    }


    /**
     * 只有被外部线程中断，才会返回false
     */
    private boolean mmapOperation() {
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            if (null == this.requestTable.get(req.getFilePath())) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();
                MappedFile mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                // 记录大于10ms的
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog() //
                        && this.messageStore.getMessageStoreConfig().isWarmMappedFileEnable()) {
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
            }

        } catch (InterruptedException e) {
            if (!stopped) {
                log.warn(this.getServiceName() + " service has exception.");
            } else {
                log.warn(getServiceName() + " service is interrupted as it's being shut down");
            }

            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
        } finally {
            if (req != null) {
                req.getCountDownLatch().countDown();
            }
        }
        return true;
    }

    class AllocateRequest implements Comparable<AllocateRequest> {
        // 文件全路径
        private String filePath;
        // 文件大小
        private int fileSize;
        // 计数器
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        // MappedFile
        private volatile MappedFile mappedFile = null;


        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }


        public String getFilePath() {
            return filePath;
        }


        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }


        public int getFileSize() {
            return fileSize;
        }


        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }


        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }


        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }


        public MappedFile getMappedFile() {
            return mappedFile;
        }


        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }


        public int compareTo(AllocateRequest other) {
            return this.fileSize < other.fileSize ? 1 : this.fileSize > other.fileSize ? -1 : 0;
        }
    }
}

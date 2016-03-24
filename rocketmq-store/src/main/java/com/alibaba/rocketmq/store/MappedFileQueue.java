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

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 存储队列，数据定时删除，无限增长<br>
 * 队列是由多个文件组成
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class MappedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);
    // 每次触发删除文件，最多删除多少个文件
    private static final int DeleteFilesBatchMax = 10;
    // 文件存储位置
    private final String storePath;
    // 每个文件的大小
    private final int mappedFileSize;
    // 各个文件
    private final List<MappedFile> mappedFiles = new ArrayList<MappedFile>();
    // 读写锁（针对mappedFiles）
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    // 预分配MappedFile对象服务
    private final AllocateMappedFileService allocateMappedFileService;
    // 刷盘刷到哪里
    private long committedWhere = 0;
    // 最后一条消息存储时间
    private volatile long storeTimestamp = 0;

    private static final String MAPPED_FILE_NAME_PATTERN_STRING = "\\d+";
    private static final Pattern MAPPED_FILE_NAME_PATTERN = Pattern.compile(MAPPED_FILE_NAME_PATTERN_STRING);


    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }


    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }


    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mappedFiles.size() <= reservedMappedFiles) {
                return null;
            }

            mfs = this.mappedFiles.toArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }


    /**
     * recover时调用，不需要加锁
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // 将文件删除掉
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }


    /**
     * 删除文件只能从头开始删
     */
    private void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MappedFile file : files) {
                    if (!this.mappedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                Matcher matcher = MAPPED_FILE_NAME_PATTERN.matcher(name);
                return matcher.matches();
            }
        });

        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                // 校验文件大小是否匹配
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                // 恢复队列
                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    /**
     * 刷盘进度落后了多少
     */
    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.committedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile();
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }


    public MappedFile getLastMappedFile() {
        return this.getLastMappedFile(0);
    }


    /**
     * 获取最后一个MappedFile对象，如果一个都没有，则新创建一个，如果最后一个写满了，则新创建一个
     *
     * @param startOffset 如果创建新的文件，起始offset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        long createOffset = -1;
        MappedFile mappedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mappedFiles.isEmpty()) {
                createOffset = startOffset - (startOffset % this.mappedFileSize);
            } else {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                mappedFile =
                        this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                                nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mapped file exception", e);
                }
            }

            if (mappedFile != null) {
                this.readWriteLock.writeLock().lock();
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mappedFile;
        }

        return mappedFileLast;
    }


    /**
     * 获取队列的最小Offset，如果队列为空，则返回-1
     */
    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mappedFiles.isEmpty()) {
                return this.mappedFiles.get(0).getFileFromOffset();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mappedFiles.isEmpty()) {
                int lastIndex = this.mappedFiles.size() - 1;
                MappedFile mappedFile = this.mappedFiles.get(lastIndex);
                return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }


    /**
     * 恢复时调用
     */
    public void deleteLastMappedFile() {
        if (!this.mappedFiles.isEmpty()) {
            int lastIndex = this.mappedFiles.size() - 1;
            MappedFile mappedFile = this.mappedFiles.get(lastIndex);
            mappedFile.destroy(1000);
            this.mappedFiles.remove(mappedFile);
            log.info("on recover, destroy a logic mapped file " + mappedFile.getFileName());
        }
    }


    /**
     * 根据文件过期时间来删除物理队列文件
     */
    public int deleteExpiredFileByTime(//
                                       final long expiredTime, //
                                       final int deleteFilesInterval, //
                                       final long intervalForcibly,//
                                       final boolean cleanImmediately//
    ) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        // 最后一个文件处于写状态，不能删除
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        for (int i = 0; i < mfsLength; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
            if (System.currentTimeMillis() >= liveMaxTimestamp//
                    || cleanImmediately) {
                if (mappedFile.destroy(intervalForcibly)) {
                    files.add(mappedFile);
                    deleteCount++;

                    if (files.size() >= DeleteFilesBatchMax) {
                        break;
                    }

                    if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                        try {
                            Thread.sleep(deleteFilesInterval);
                        } catch (InterruptedException e) {
                        }
                    }
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    /**
     * 根据物理队列最小Offset来删除逻辑队列
     *
     * @param offset 物理队列最小offset
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            // 最后一个文件处于写状态，不能删除
            int mfsLength = mfs.length - 1;

            // 这里遍历范围 0 ... last - 1
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = true;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    // 当前文件是否可以删除
                    destroy = (maxOffsetInLogicQueue < offset);
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mapped file max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    /**
     * 返回值表示是否全部刷盘完成
     *
     * @return
     */
    public boolean commit(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, true);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.commit(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }


    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MappedFile mappedFile = this.getFirstMappedFile();

            if (mappedFile != null) {
                int index = (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                if (!returnFirstOnNotFound && (index < 0 || index >= this.mappedFiles.size())) {
                    logError.warn("findMappedFileByOffset offset not matched, request Offset: {}, index: {}, " +
                                    "mappedFileSize: {}, mappedFiles count: {}, StackTrace: {}",//
                            offset,//
                            index,//
                            this.mappedFileSize,//
                            this.mappedFiles.size(),//
                            UtilAll.currentStackTrace());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MappedFile getFirstMappedFile() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }

        return this.mappedFiles.get(0);
    }


    public MappedFile getLastMappedFile2() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }
        return this.mappedFiles.get(this.mappedFiles.size() - 1);
    }


    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }


    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFileOnLock();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mapped file was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.warn("the mapped file re-delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tempMappedFiles = new ArrayList<MappedFile>();
                    tempMappedFiles.add(mappedFile);
                    this.deleteExpiredFile(tempMappedFiles);
                } else {
                    log.warn("the mapped file re-delete Failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public MappedFile getFirstMappedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMappedFile();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    /**
     * 关闭队列，队列数据还在，但是不能访问
     */
    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    /**
     * 销毁队列，队列数据被删除，此函数有可能不成功
     */
    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.committedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }


    public int getMappedFileSize() {
        return mappedFileSize;
    }
}

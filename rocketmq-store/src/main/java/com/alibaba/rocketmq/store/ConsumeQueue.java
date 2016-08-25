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

import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * 消费队列实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class ConsumeQueue {

    // 存储单元大小, physical Offset(long, 8) + message size(int, 4) + tag hash code(long, 8)
    public static final int CQStoreUnitSize = 20;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 存储消息索引的队列
    private final MappedFileQueue mappedFileQueue;
    // Topic
    private final String topic;
    // queueId
    private final int queueId;
    // 写索引时用到的ByteBuffer
    private final ByteBuffer byteBufferIndex;
    // 配置
    private final String storePath;
    private final int mappedFileSize;
    // 最后一个消息对应的物理Offset
    private long maxPhysicOffset = -1;
    // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
    // 实际使用需要除以 StoreUnitSize
    private volatile long minLogicOffset = 0;


    public ConsumeQueue(//
            final String topic,//
            final int queueId,//
            final String storePath,//
            final int mappedFileSize,//
            final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath//
                + File.separator + topic//
                + File.separator + queueId;//

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQStoreUnitSize);
    }


    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }


    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQStoreUnitSize) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 说明当前存储单元有效
                    // TODO 这样判断有效是否合理？
                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQStoreUnitSize;
                        this.maxPhysicOffset = offset;
                    }
                    else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                // 走到文件末尾，切换至下一个文件
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    }
                    else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                }
                else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    /**
     * 二分查找查找消息发送时间最接近timestamp逻辑队列的offset
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            // low:第一个索引信息的起始位置
            // minLogicOffset有设置值则从
            // minLogicOffset-mappedFile.getFileFromOffset()位置开始才是有效值
            int low =
                    minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile
                        .getFileFromOffset()) : 0;

            // high:最后一个索引信息的起始位置
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;

            // 取出该mappedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQStoreUnitSize;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQStoreUnitSize) * CQStoreUnitSize;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();

                        // 比较时间, 折半
                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            // 没有从物理文件找到消息，此时直接返回0
                            return 0;
                        }
                        else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        }
                        else if (storeTime > timestamp) {
                            high = midOffset - CQStoreUnitSize;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        }
                        else {
                            low = midOffset + CQStoreUnitSize;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {
                        // 查询的时间正好是消息索引记录写入的时间
                        offset = targetOffset;
                    }
                    else {
                        if (leftIndexValue == -1) {
                            // timestamp 时间小于该MappedFile中第一条记录记录的时间
                            offset = rightOffset;
                        }
                        else if (rightIndexValue == -1) {
                            // timestamp 时间大于该MappedFile中最后一条记录记录的时间
                            offset = leftOffset;
                        }
                        else {
                            // 取最接近timestamp的offset
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQStoreUnitSize;
                }
                finally {
                    sbr.release();
                }
            }
        }

        // 映射文件被标记为不可用时返回0
        return 0;
    }


    /**
     * 根据物理Offset删除无效逻辑文件
     */
    public void truncateDirtyLogicFiles(long phyOffset) {
        // 逻辑队列每个文件大小
        int logicFileSize = this.mappedFileSize;

        // 先改变逻辑队列存储的物理Offset
        this.maxPhysicOffset = phyOffset - 1;

        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile2();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                // 先将Offset清空
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQStoreUnitSize) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    byteBuffer.getLong();

                    // 逻辑文件起始单元
                    if (0 == i) {
                        if (offset >= phyOffset) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        }
                        else {
                            int pos = i + CQStoreUnitSize;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            this.maxPhysicOffset = offset;
                        }
                    }
                    // 逻辑文件中间单元
                    else {
                        // 说明当前存储单元有效
                        if (offset >= 0 && size > 0) {
                            // 如果逻辑队列存储的最大物理offset大于物理队列最大offset，则返回
                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQStoreUnitSize;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            this.maxPhysicOffset = offset;

                            // 如果最后一个MappedFile扫描完，则返回
                            if (pos == logicFileSize) {
                                return;
                            }
                        }
                        else {
                            return;
                        }
                    }
                }
            }
            else {
                break;
            }
        }
    }


    /**
     * 返回最后一条消息对应物理队列的Next Offset
     */
    public long getLastOffset() {
        // 物理队列Offset
        long lastOffset = -1;
        // 逻辑队列每个文件大小
        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile2();
        if (mappedFile != null) {
            // 找到写入位置对应的索引项的起始位置
            int position = mappedFile.getWrotePosition() - CQStoreUnitSize;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQStoreUnitSize) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                // 说明当前存储单元有效
                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                }
                else {
                    break;
                }
            }
        }

        return lastOffset;
    }


    public boolean commit(final int flushLeastPages) {
        return this.mappedFileQueue.commit(flushLeastPages);
    }


    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQStoreUnitSize);
        // 无论是否删除文件，都需要纠正下最小值，因为有可能物理文件删除了，
        // 但是逻辑文件一个也删除不了
        this.correctMinOffset(offset);
        return cnt;
    }


    /**
     * 逻辑队列的最小Offset要比传入的物理最小phyMinOffset大
     */
    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFileOnLock();
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    // 有消息存在
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("compute logics min offset: " + this.getMinOffsetInQueue() + ", topic: "
                                    + this.topic + ", queueId: " + this.queueId);
                            break;
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    result.release();
                }
            }
        }
    }


    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQStoreUnitSize;
    }


    public void putMessagePositionInfoWrapper(long offset, int size, long tagsCode, long storeTimestamp,
                                              long logicOffset) {
        final int MaxRetries = 5;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isWritable();
        for (int i = 0; i < MaxRetries && canWrite; i++) {
            boolean result = this.putMessagePositionInfo(offset, size, tagsCode, logicOffset);
            if (result) {
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);
                return;
            }
            // 只有一种情况会失败，创建新的MappedFile时报错或者超时
            else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + offset
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }


    /**
     * 存储一个20字节的信息，putMessagePositionInfo只有一个线程调用，所以不需要加锁
     * 
     * @param offset
     *            消息对应的CommitLog offset
     * @param size
     *            消息在CommitLog存储的大小
     * @param tagsCode
     *            tags 计算出来的长整数
     * @return 是否成功
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {
        // 在数据恢复时会走到这个流程
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQStoreUnitSize);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQStoreUnitSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            // 纠正MappedFile逻辑队列索引顺序
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
                if (expectLogicOffset != currentLogicOffset) {
                    // XXX: warn and notify me
                    logError
                        .warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",//
                            expectLogicOffset, //
                            currentLogicOffset,//
                            this.topic,//
                            this.queueId,//
                            expectLogicOffset - currentLogicOffset//
                        );
                }
            }

            // 记录物理队列最大offset
            this.maxPhysicOffset = offset;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }

        return false;
    }


    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQStoreUnitSize);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQStoreUnitSize) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }


    /**
     * 返回Index Buffer
     * 
     * @param startIndex
     *            起始偏移量索引
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQStoreUnitSize;
        if (offset >= getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }


    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQStoreUnitSize;
        return (index + totalUnitsInFile - index % totalUnitsInFile);
    }


    public String getTopic() {
        return topic;
    }


    public int getQueueId() {
        return queueId;
    }


    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }


    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }


    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
    }


    public long getMinLogicOffset() {
        return minLogicOffset;
    }


    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }


    /**
     * 获取当前队列中的消息总数
     */
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }


    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQStoreUnitSize;
    }
}

package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.rocketmq.store.AppendMessageStatus.END_OF_FILE;
import static com.alibaba.rocketmq.store.CommitLog.MessageMagicCode;

public class MappedFileLocalMessageStore implements LocalMessageStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedFileLocalMessageStore.class);

    private final AppendMessageCallback appendMessageCallback;

    private final MappedFileQueue mappedFileQueue;

    private final AllocateMappedFileService allocateMappedFileService;

    private final File abortFile;

    private final AtomicLong readOffset = new AtomicLong(0L);
    private final MappedByteBuffer checkpointByteBuffer;

    // 文件末尾空洞对应的MAGIC CODE cbd43194
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;

    private final static int MAPPED_FILE_SIZE = 1024 * 1024 * 1024;

    private final static int MAX_MESSAGE_SIZE = 1024 * 512;

    public MappedFileLocalMessageStore(final String storePath) throws IOException {

        File storeFile = new File(storePath);
        if (!storeFile.exists()) {
            if (!storeFile.mkdirs()) {
                throw new IOException("Unable to create store directory");
            }
        } else if (storeFile.isFile()) {
            throw new IOException("There is an existing file with the same name. Unable to create store");
        }

        allocateMappedFileService = new AllocateMappedFileService();
        appendMessageCallback = new AppendMessageCallbackImpl();
        mappedFileQueue = new MappedFileQueue(storePath, MAPPED_FILE_SIZE, allocateMappedFileService);

        File checkpoint = new File(storePath, "checkpoint.data");
        boolean initCheckPoint = false;
        if (!checkpoint.exists()) {

            if (!checkpoint.createNewFile()) {
                throw new IOException("Unable to create checkpoint file");
            }

        } else {
            initCheckPoint = true;
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(checkpoint, "rw");
        checkpointByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);

        if (initCheckPoint) {
            readOffset.set(checkpointByteBuffer.getLong());
        }

        abortFile = new File(storePath, ".abort");
    }

    @Override
    public void start() throws IOException {
        allocateMappedFileService.start();
        if (!mappedFileQueue.load()) {
            throw new IOException("Failed to load mapped file queue");
        }

        if (isPreviousShutdownNormal()) {
            recoverNormally();
            createAbortFile();
        } else {
            recoverAbnormally();
        }
    }

    @Override
    public boolean stash(Message message) {
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        if (null == mappedFile) {
            LOGGER.error("Unable to create mapped file");
            return false;
        }

        // We need to synchronize while writing data to commit log.
        synchronized (this) {
            AppendMessageResult appendMessageResult = mappedFile.appendMessage(message, appendMessageCallback);
            switch (appendMessageResult.getStatus()) {
                case END_OF_FILE:

                    // 创建新文件，重新写消息
                    mappedFile = mappedFileQueue.getLastMappedFile();
                    if (null == mappedFile) {
                        LOGGER.error("Unable to create mapped file");
                        return false;
                    }

                    appendMessageResult = mappedFile.appendMessage(message, appendMessageCallback);
                    switch (appendMessageResult.getStatus()) {
                        case PUT_OK:
                            mappedFile.commit(1);
                            return true;

                        default:
                            return false;
                    }

                case PUT_OK:
                    mappedFile.commit(1);
                    return true;

                default:
                    return false;
            }
        }
    }

    @Override
    public int getNumberOfMessageStashed() {
        return 0;
    }

    @Override
    public Message[] pop(int n) {
        if (readOffset.get() >= mappedFileQueue.getMaxOffset()) {
            return new Message[0];
        }

        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(readOffset.get(), false);
        if (null == mappedFile) {
            return new Message[0];
        }

        int logicalOffset = (int)(readOffset.get() - mappedFile.getFileFromOffset());
        SelectMappedBufferResult selectMappedBufferResult = null;

        try {
            selectMappedBufferResult = mappedFile.selectMappedBuffer(logicalOffset);

            List<Message> messages = new ArrayList<>(n);
            ByteBuffer byteBuffer = selectMappedBufferResult.getByteBuffer();
            for (int i = 0; i < n && byteBuffer.hasRemaining(); i++) {
                int pos = byteBuffer.position();
                MessageExt messageExt = MessageDecoder.decode(byteBuffer, true);
                messages.add(messageExt);
                readOffset.addAndGet(byteBuffer.position() - pos);
            }
            mappedFileQueue.deleteExpiredFilesByPhysicalOffset(readOffset.get());
            return messages.toArray(new Message[0]);
        } finally {
            if (null != selectMappedBufferResult) {
                selectMappedBufferResult.release();
            }
            saveCheckPoint();
        }
    }

    private void saveCheckPoint() {
        checkpointByteBuffer.clear();
        checkpointByteBuffer.putLong(readOffset.get());
        checkpointByteBuffer.force();
    }

    @Override
    public void close() throws InterruptedException {
        checkpointByteBuffer.force();
        mappedFileQueue.shutdown(3 * 1000);
        removeAbortFile();
    }

    private class AppendMessageCallbackImpl implements AppendMessageCallback {

        // 文件末尾空洞最小定长
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        // 存储消息内容
        private final ByteBuffer msgStoreItemMemory;

        public AppendMessageCallbackImpl() {
            this.msgStoreItemMemory = ByteBuffer.allocate(MAX_MESSAGE_SIZE + END_FILE_MIN_BLANK_LENGTH);
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, Object msg) {
            MessageExt messageExt;
            if (msg instanceof Message) {
                messageExt = StoreHelper.wrap((Message)msg);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            ByteBuffer data = MessageEncoder.encode(messageExt);

            int msgLen = data.capacity();
            // 判断是否有足够空余空间
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(maxBlank);

                // 1 TOTALSIZE
                msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                msgStoreItemMemory.putInt(BlankMagicCode);
                // 3 剩余空间可能是任何值
                //

                // 此处长度特意设置为maxBlank
                byteBuffer.put(msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(END_OF_FILE, fileFromOffset + byteBuffer.position(), maxBlank, null,
                        System.currentTimeMillis(), 0L);
            }

            if (maxBlank < data.capacity()) {
                AppendMessageResult result = new AppendMessageResult(END_OF_FILE);
                return result;
            }

            byteBuffer.put(data);
            long wroteOffset = fileFromOffset + byteBuffer.position() + data.capacity();
            AppendMessageResult result =
                    new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, data.capacity(), null,
                            System.currentTimeMillis(), 0L);

            return result;
        }
    }

    private boolean isPreviousShutdownNormal() {
        return !abortFile.exists();
    }

    private void createAbortFile() throws IOException {
        if (abortFile.exists()) {
            return;
        }

        abortFile.createNewFile();
    }

    private void removeAbortFile() {
        abortFile.deleteOnExit();
    }

    /**
     * 正常退出时，数据恢复，所有内存数据都已经刷盘
     */
    private void recoverNormally() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = checkMessageAndReturnSize(byteBuffer, false, true);
                int size = dispatchRequest.getMsgSize();
                // 正常数据
                if (size > 0) {
                    mappedFileOffset += size;
                }
                // 文件中间读到错误
                else if (size == -1) {
                    LOGGER.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 当前条件分支不可能发生
                        LOGGER.info("recover last 3 physics file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        LOGGER.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }

    /**
     * 服务端使用 检查消息并返回消息大小
     *
     * @return 0 表示走到文件末尾 >0 正常消息 -1 消息校验失败
     */
    private DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            java.nio.ByteBuffer byteBufferMessage =
                    ((AppendMessageCallbackImpl)appendMessageCallback).getMsgStoreItemMemory();
            byte[] bytesContent = byteBufferMessage.array();

            // 1 TOTALSIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGICCODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MessageMagicCode:
                    break;
                case BlankMagicCode:
                    return new DispatchRequest(0);
                default:
                    LOGGER.warn("found an illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1);
            }

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();

            // 5 FLAG
            int flag = byteBuffer.getInt();
            flag = flag + 0;

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            bornTimeStamp = bornTimeStamp + 0;

            // 10 BORNHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();

            // 12 STOREHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    // 校验CRC
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            LOGGER.warn("CRC check failed " + crc + " " + bodyCRC);
                            return new DispatchRequest(-1);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen);

            long tagsCode = 0;
            String keys = "";

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength);
                Map<String, String> propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode =
                            MessageExtBrokerInner.tagsString2tagsCode(
                                    MessageExt.parseTopicFilterType(sysFlag), tags);
                }
            }

            return new DispatchRequest(//
                    topic,// 1
                    queueId,// 2
                    physicOffset,// 3
                    totalSize,// 4
                    tagsCode,// 5
                    storeTimestamp,// 6
                    queueOffset,// 7
                    keys,// 8
                    sysFlag,// 9
                    preparedTransactionOffset// 10
            );
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return new DispatchRequest(-1);
    }

    private void recoverAbnormally() {
        // 根据最小时间戳来恢复
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 寻找从哪个文件开始恢复
            int index = 0;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, false, true);
                int size = dispatchRequest.getMsgSize();
                // 正常数据
                if (size > 0) {
                    mappedFileOffset += size;
                }
                // 文件中间读到错误
                else if (size == -1) {
                    LOGGER.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 当前条件分支正常情况下不应该发生
                        LOGGER.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        LOGGER.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
        // 物理文件都被删除情况下
        else {
            this.mappedFileQueue.setCommittedWhere(0);
        }
    }
}

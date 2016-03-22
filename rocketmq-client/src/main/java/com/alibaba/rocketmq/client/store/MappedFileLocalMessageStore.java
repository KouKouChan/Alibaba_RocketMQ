package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageEncoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.rocketmq.store.AppendMessageStatus.END_OF_FILE;

public class MappedFileLocalMessageStore implements LocalMessageStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedFileLocalMessageStore.class);

    private final AppendMessageCallback appendMessageCallback;

    private final MappedFileQueue mappedFileQueue;

    private final AllocateMappedFileService allocateMappedFileService;

    private final AtomicLong readOffset = new AtomicLong(0L);
    private final MappedByteBuffer checkpointByteBuffer;

    private final static int MAPPED_FILE_SIZE = 1024 * 1024 * 1024;

    public MappedFileLocalMessageStore(final String storePath) throws IOException {
        allocateMappedFileService = new AllocateMappedFileService();
        appendMessageCallback = new AppendMessageCallbackImpl();
        mappedFileQueue = new MappedFileQueue(storePath, MAPPED_FILE_SIZE, allocateMappedFileService);

        File checkpoint = new File(storePath, "checkpoint.data");
        boolean initCheckPoint = false;
        if (!checkpoint.exists()) {
            checkpoint.createNewFile();
        } else {
            initCheckPoint = true;
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(checkpoint, "rw");
        checkpointByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);

        if (initCheckPoint) {
            readOffset.set(checkpointByteBuffer.getLong());
        }

    }

    @Override
    public void start() throws IOException {
        allocateMappedFileService.start();
        if (!mappedFileQueue.load()) {
            throw new IOException("Failed to load mapped file queue");
        }
    }

    @Override
    public boolean stash(Message message) {
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        if (null == mappedFile) {
            LOGGER.error("Unable to create mapped file");
            return false;
        }
        AppendMessageResult appendMessageResult = mappedFile.appendMessage(message, appendMessageCallback);
        switch (appendMessageResult.getStatus()) {
            case END_OF_FILE:

                // 创建新文件，重新写消息
                mappedFile = this.mappedFileQueue.getLastMappedFile();
                if (null == mappedFile) {
                    LOGGER.error("Unable to create mapped file");
                    return false;
                }

                mappedFile.appendMessage(message, this.appendMessageCallback);
                mappedFile.commit(1);
                return true;

            case PUT_OK:
                mappedFile.commit(1);
                return true;

            default:
                return false;
        }

    }

    @Override
    public int getNumberOfMessageStashed() {
        return 0;
    }

    @Override
    public Message[] pop(int n) {
        return new Message[0];
    }

    @Override
    public void close() throws InterruptedException {
        mappedFileQueue.shutdown(3 * 1000);
    }

    private class AppendMessageCallbackImpl implements AppendMessageCallback {

        // 文件末尾空洞最小定长
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        // 文件末尾空洞对应的MAGIC CODE cbd43194
        private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;

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
}

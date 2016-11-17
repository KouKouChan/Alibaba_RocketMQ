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
import com.alibaba.rocketmq.store.config.FlushDiskType;
import com.alibaba.rocketmq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Pagecache文件访问封装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class MappedFile extends ReferenceResource {

    /**
     * Default page cache size: 4k.
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    // 当前JVM中映射的虚拟内存总大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 当前JVM中mmap句柄数量
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    // 映射的文件名
    private final String fileName;

    // 映射的起始偏移量
    private final long fileFromOffset;
    // 映射的文件大小，定长
    private final int fileSize;
    // 映射的文件
    private final File file;
    // 映射的内存对象，position永远不变
    private final MappedByteBuffer mappedByteBuffer;
    // 当前写到什么位置
    private final AtomicInteger wrotePosition = new AtomicInteger(0);
    // Flush到什么位置
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    // 映射的FileChannel对象
    private FileChannel fileChannel;
    // 最后一条消息存储时间
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;


    public MappedFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        }
        catch (FileNotFoundException e) {
            LOGGER.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        }
        catch (IOException e) {
            LOGGER.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
        finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }


    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                LOGGER.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }


    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }


    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        }
        catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }


    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        // JDK7中将DirectByteBuffer类中的viewedBuffer方法换成了attachment方法
        Method[] methods = buffer.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }


    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }


    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }


    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }


    public String getFileName() {
        return fileName;
    }


    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }


    public FileChannel getFileChannel() {
        return fileChannel;
    }


    /**
     * 向MappedBuffer追加消息<br>
     * 
     * @param msg
     *            要追加的消息
     * @param cb
     *            用来对消息进行序列化，尤其对于依赖MappedFile Offset的属性进行动态序列化
     * @return 是否成功，写入多少数据
     */
    public AppendMessageResult appendMessage(final Object msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;

        int currentPos = this.wrotePosition.get();

        // 表示有空余空间
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                    msg);
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        // 上层应用应该保证不会走到这里
        LOGGER.error("MappedFile.appendMessage return null, wrotePosition: " + currentPos + " fileSize: "
                + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    /**
     * 文件起始偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }


    /**
     * 向存储层追加数据，一般在SLAVE存储结构中使用
     * 
     * @return 返回写入了多少数据
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        // 表示有空余空间
        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }


    /**
     * 消息刷盘
     * 
     * @param flushLeastPages
     *            至少刷几个page
     * @return
     */
    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = this.wrotePosition.get();
                this.mappedByteBuffer.force();
                this.committedPosition.set(value);
                this.release();
            }
            else {
                LOGGER.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                this.committedPosition.set(this.wrotePosition.get());
            }
        }

        return this.getCommittedPosition();
    }


    public int getCommittedPosition() {
        return committedPosition.get();
    }


    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }


    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        // 如果当前文件已经写满，应该立刻刷盘
        if (this.isFull()) {
            return true;
        }

        // 只有未刷盘数据满足指定page数目才刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }


    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }


    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        // 有消息
        if ((pos + size) <= this.wrotePosition.get()) {
            // 从MappedBuffer读
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
            else {
                LOGGER.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            LOGGER.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }


    /**
     * 读逻辑分区
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        if (pos < this.wrotePosition.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePosition.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }


    @Override
    public boolean cleanup(final long currentRef) {
        // 如果没有被shutdown，则不可以unmap文件，否则会crash
        if (this.isAvailable()) {
            LOGGER.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmaping.");
            return false;
        }

        // 如果已经cleanup，再次操作会引起crash
        if (this.isCleanupOver()) {
            LOGGER.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            // 必须返回true
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        LOGGER.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }


    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个
     * 
     * @return 是否被destroy成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                LOGGER.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                LOGGER.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getCommittedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            }
            catch (Exception e) {
                LOGGER.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        }
        else {
            LOGGER.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }


    public int getWrotePosition() {
        return wrotePosition.get();
    }


    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }


    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }


    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }


    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            LOGGER.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            LOGGER.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        LOGGER.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    /**
     * This method brings about overwhelming overhead. DO NOT USE.
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                LOGGER.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            LOGGER.info("mapped file warm up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        LOGGER.info("mapped file warm up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }
}

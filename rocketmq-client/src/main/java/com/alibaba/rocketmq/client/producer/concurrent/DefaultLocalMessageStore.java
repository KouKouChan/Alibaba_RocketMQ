package com.alibaba.rocketmq.client.producer.concurrent;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.ClientStatus;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageEncoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultLocalMessageStore implements LocalMessageStore {

    private static final String DEFAULT_STORE_LOCATION = "/dianyi/data/";

    private static final String LOCAL_MESSAGE_STORE_FOLDER_NAME = ".localMessageStore";

    private static final String ABORT_FILE_NAME = ".abort";

    private static final String CONFIG_FILE_NAME = ".config";

    private static final Logger LOGGER = ClientLogger.getLog();

    private static final int MESSAGES_PER_FILE = 100000;

    private final AtomicLong writeIndex = new AtomicLong(0L);
    private final AtomicLong writeOffSet = new AtomicLong(0L);

    private final AtomicLong readIndex = new AtomicLong(0L);
    private final AtomicLong readOffSet = new AtomicLong(0L);

    private File localMessageStoreDirectory;

    private ConcurrentHashMap<Long, File> messageStoreNameFileMapping = new ConcurrentHashMap<Long, File>();

    private File configFile;

    private RandomAccessFile writeRandomAccessFile;

    private ReentrantLock lock = new ReentrantLock();

    private static final int QUEUE_CAPACITY = 1000;

    private LinkedBlockingQueue<MessageExt> messageQueue = new LinkedBlockingQueue<MessageExt>(QUEUE_CAPACITY);

    private static final int HIGH_QUEUE_LEVEL = (int)(QUEUE_CAPACITY * 0.8);

    private static final int LOW_QUEUE_LEVEL = (int)(QUEUE_CAPACITY * 0.3);

    private volatile ClientStatus status = ClientStatus.CREATED;

    private static final int UPDATE_CONFIG_PER_FLUSHING_NUMBER_OF_MESSAGE = 500;

    private static final float DISK_HIGH_WATER_LEVEL = 0.75F;

    private static final float DISK_WARNING_WATER_LEVEL = 0.65F;

    private volatile long lastFlushTime = -1;

    private volatile long lastWarnTime = -1;

    private static final String ACCESS_FILE_MODE = "rws";

    private FlushDiskService flushDiskService;

    /**
     * Flush request.
     */
    class FlushDiskRequest {

        /**
         * Indicate whether we need to flush all message to disk.
         */
        private boolean forceful;

        public FlushDiskRequest() {
        }

        public FlushDiskRequest(boolean forceful) {
            this.forceful = forceful;
        }

        public boolean isForceful() {
            return forceful;
        }
    }


    class FlushDiskService extends ServiceThread {

        private volatile List<FlushDiskRequest> requestsWrite = new ArrayList<FlushDiskRequest>();
        private volatile List<FlushDiskRequest> requestsRead = new ArrayList<FlushDiskRequest>();

        @Override
        public String getServiceName() {
            return FlushDiskService.class.getSimpleName();
        }

        public void putRequest(final FlushDiskRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!hasNotified) {
                    hasNotified = true;
                    notify();
                }
            }
        }


        private void swapRequests() {
            List<FlushDiskRequest> tmp = requestsWrite;
            requestsWrite = requestsRead;
            requestsRead = tmp;
        }

        @Override
        public void run() {
            LOGGER.info(getServiceName() + " starts.");
            while (!isStopped()) {
                waitForRunning(0);
                doFlush();
            }

            // 在正常shutdown情况下，等待请求到来，然后再刷盘
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOGGER.error("", e);
            }


            synchronized (this) {
                putRequest(new FlushDiskRequest(true));
                swapRequests();
            }

            doFlush();
            LOGGER.info(getServiceName() + " terminated.");
        }


        public void doFlush() {
            if (!requestsRead.isEmpty()) {
                boolean flushed = false;

                // Check if there is any request to flush all messages to disk.
                for (FlushDiskRequest request : requestsRead) {
                    if (request.isForceful()) {
                        flushed = true;
                        flush(true);
                        break;
                    }
                }

                // We need to perform a normal maintaining flush.
                if (!flushed) {
                    flush();
                }

                requestsRead.clear();
            }
        }

        @Override
        protected void onWaitEnd() {
            swapRequests();
        }
    }

    public static File getLocalMessageStoreDirectory(String storeName) {
        //For convenience of development.
        String storeLocation = System.getProperty("defaultLocalMessageStoreLocation", DEFAULT_STORE_LOCATION);
        if (DEFAULT_STORE_LOCATION.equals(storeLocation)) {
            File defaultStoreLocation = new File(DEFAULT_STORE_LOCATION);
            if (!defaultStoreLocation.exists()) {
                storeLocation = System.getProperty("user.home") + File.separator + LOCAL_MESSAGE_STORE_FOLDER_NAME;
            } else {
                storeLocation = storeLocation.endsWith(File.separator)
                        ? storeLocation + LOCAL_MESSAGE_STORE_FOLDER_NAME
                        : storeLocation + File.separator + LOCAL_MESSAGE_STORE_FOLDER_NAME;
            }
        }
        return new File(storeLocation, storeName);
    }

    public DefaultLocalMessageStore(String storeName) throws IOException {
        localMessageStoreDirectory = getLocalMessageStoreDirectory(storeName);

        if (!localMessageStoreDirectory.exists()) {
            if (!localMessageStoreDirectory.mkdirs()) {
                throw new IOException("Local message store directory does not exist and unable to create one");
            }
        }

        if (isLastShutdownAbort()) {
            init(true);
        } else {
            init(false);
        }

        try {
            createAbortFile();
        } catch (IOException e) {
            LOGGER.error("Failed to create abort file.", e);
            throw e;
        }

        flushDiskService = new FlushDiskService();
        flushDiskService.start();

        status = ClientStatus.ACTIVE;
        LOGGER.info("Local Message store starts to operate.");
    }

    private boolean isLastShutdownAbort() {
        File abortFile = new File(localMessageStoreDirectory, ABORT_FILE_NAME);
        return abortFile.exists();
    }

    private void createAbortFile() throws IOException {
        File abortFile = new File(localMessageStoreDirectory, ABORT_FILE_NAME);
        if (abortFile.exists()) {
            LOGGER.error("Abort file already exists.");
        } else {
            if (!abortFile.createNewFile()) {
                LOGGER.error("Failed to create abort file");
            } else {
                LOGGER.info("Abort file created: " + abortFile.getAbsolutePath());
            }
        }
    }

    private void deleteAbortFile() {
        File abortFile = new File(localMessageStoreDirectory, ABORT_FILE_NAME);
        if (abortFile.exists()) {
            if (!abortFile.delete()) {
                LOGGER.error("Failed to delete abort file");
            } else {
                LOGGER.info("Abort file deleted");
            }
        } else {
            LOGGER.error("Abort file does not exist");
        }
    }

    /**
     * This method will execute on startup.
     */
    private void init(boolean needToRecoverData) throws IOException {
        configFile = new File(localMessageStoreDirectory, CONFIG_FILE_NAME);
        String[] dataFiles = getMessageDataFiles();
        if (configFile.exists()) {
            if (!configFile.canRead()) {
                LOGGER.error("Unable to read {}. No read permission.", configFile.getAbsolutePath());
                throw new IOException("No read permission to " + configFile.getAbsolutePath());
            }

            if (!configFile.canWrite()) {
                LOGGER.error("Unable to write {}. No write permission.", configFile.getAbsolutePath());
                throw new IOException("No write permission to " + configFile.getAbsolutePath());
            }

            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(configFile);
                Properties properties = new Properties();
                properties.load(inputStream);

                writeIndex.set(null == properties.getProperty("writeIndex") ? 0L :
                        Long.parseLong(properties.getProperty("writeIndex")));
                writeOffSet.set(null == properties.getProperty("writeOffSet") ? 0L :
                        Long.parseLong(properties.getProperty("writeOffSet")));
                readIndex.set(null == properties.getProperty("readIndex") ? 0L :
                        Long.parseLong(properties.getProperty("readIndex")));
                readOffSet.set(null == properties.getProperty("readOffSet") ? 0L :
                        Long.parseLong(properties.getProperty("readOffSet")));

                for (String dataFile : dataFiles) {
                    messageStoreNameFileMapping.putIfAbsent(Long.parseLong(dataFile),
                            new File(localMessageStoreDirectory, dataFile));
                }

                if (!isMessageDataContinuous(dataFiles)) {
                    throw new RuntimeException("Message data files are corrupted and unable to recover automatically");
                }

                if (!needToRecoverData) {
                    needToRecoverData = !verifyIndexes(dataFiles);
                }

                if (needToRecoverData) {
                    LOGGER.info("Begin to recover data.");
                    long readIndexLong = readIndex.longValue();
                    long writeIndexLong = writeIndex.longValue();

                    long minMessageFileName = 0;
                    long maxMessageFileName = 0;
                    if (dataFiles.length > 0) {
                        minMessageFileName = Long.parseLong(dataFiles[0]);
                        maxMessageFileName = Long.parseLong(dataFiles[dataFiles.length - 1]);
                    }

                    //Fix possible discrepancies.
                    if (!messageStoreNameFileMapping.isEmpty()) {
                        if (readIndexLong < minMessageFileName) {
                            readIndex.set(minMessageFileName - 1);
                            readOffSet.set(0);
                        }

                        if (writeIndexLong < maxMessageFileName
                                || writeIndexLong > maxMessageFileName + MESSAGES_PER_FILE) {
                            writeIndex.set(maxMessageFileName - 1);
                            writeOffSet.set(0);
                            recoverWriteAheadData(messageStoreNameFileMapping.get(maxMessageFileName));
                        }
                    } else {
                        //Reset all indexes and offsets to 0 if there is any discrepancy and there is no message
                        // data file at the same tile.
                        if (readIndex.longValue() != writeIndex.longValue()
                                || readOffSet.longValue() != writeOffSet.longValue()) {
                            readIndex.set(0);
                            readOffSet.set(0);
                            writeIndex.set(0);
                            writeOffSet.set(0);
                            messageStoreNameFileMapping.clear();
                        }
                    }

                    updateConfig();
                    if (isLastShutdownAbort()) {
                        deleteAbortFile();
                    }
                    LOGGER.info("Data Recovery completes.");
                }

                File lastWrittenFileName = messageStoreNameFileMapping
                        .get(writeIndex.longValue() / MESSAGES_PER_FILE * MESSAGES_PER_FILE + 1);

                if (null == lastWrittenFileName && writeIndex.longValue() % MESSAGES_PER_FILE != 0) {
                    throw new RuntimeException("The file being written to is missing");
                }

                if (null != lastWrittenFileName) {
                    checkFileToWrite(lastWrittenFileName);
                    writeRandomAccessFile = new RandomAccessFile(lastWrittenFileName, ACCESS_FILE_MODE);
                    if (writeOffSet.longValue() > 0) {
                        writeRandomAccessFile.seek(writeOffSet.longValue());
                    }
                }
            } catch (FileNotFoundException e) {
                LOGGER.error("Initializing default local store fails.", e);
                throw e;
            } catch (IOException e) {
                LOGGER.error("Initializing default local store fails.", e);
                throw e;
            } finally {
                if (null != inputStream) {
                    inputStream.close();
                }
            }
        } else {
            LOGGER.info("Begin to recover data as there is no configuration file");
            //There is no configuration file.
            if (!isMessageDataContinuous(dataFiles)) {
                throw new RuntimeException("Message data files are corrupted and unable to recover automatically");
            }

            if (dataFiles.length > 0) {
                readIndex.set(Long.parseLong(dataFiles[0]) - 1);
                readOffSet.set(0);

                final int len = dataFiles.length;
                writeIndex.set(Long.parseLong(dataFiles[len - 1]) - 1);
                writeOffSet.set(0);
                recoverWriteAheadData(new File(localMessageStoreDirectory, dataFiles[len - 1]));

                for (String dataFile : dataFiles) {
                    messageStoreNameFileMapping.putIfAbsent(Long.parseLong(dataFile),
                            new File(localMessageStoreDirectory, dataFile));
                }
            }
            updateConfig();

            if (isLastShutdownAbort()) {
                deleteAbortFile();
            }

            LOGGER.info("Data recovery completes.");
        }

        cleanDeprecatedData();
    }

    private void cleanDeprecatedData() throws IOException {
        String[] dataFiles = getMessageDataFiles();
        //Remove possibly existing deprecated message data files.
        LOGGER.info("Start cleaning deprecated message data files.");
        for (String dataFile : dataFiles) {
            long dataFileLong = Long.parseLong(dataFile);
            if (dataFileLong < readIndex.longValue() - MESSAGES_PER_FILE) {
                File messageDataFile = new File(localMessageStoreDirectory, dataFile);
                if (messageDataFile.delete()) {
                    messageStoreNameFileMapping.remove(dataFileLong);
                } else {
                    LOGGER.error("Failed to delete deprecated message data file: {}", messageDataFile.getCanonicalPath());
                    throw new IOException("Failed to delete file: " + messageDataFile.getAbsoluteFile());
                }
            } else {
                break;
            }
        }
        LOGGER.info("Completion of cleaning deprecated message data files.");
    }

    private String[] getMessageDataFiles() {
       String[] dataFiles = localMessageStoreDirectory.list(new FilenameFilter() {
           @Override
           public boolean accept(File dir, String name) {
               return name.matches("\\d+");
           }
       });

        Arrays.sort(dataFiles, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Long.parseLong(o1) < Long.parseLong(o2) ? -1 : 1;
            }
        });

        return dataFiles;
    }

    private boolean verifyIndexes(String[] dataFiles) {
        boolean indexesValid = true;

        if (0 == dataFiles.length) {
            return true;
        }

        long min = Long.parseLong(dataFiles[0]);

        if (readIndex.longValue() < min || readIndex.longValue() > min + MESSAGES_PER_FILE) {
            indexesValid = false;
            LOGGER.error("Read index found invalid");
        }

        long max = Long.parseLong(dataFiles[dataFiles.length - 1]);

        if (writeIndex.longValue() < max || writeIndex.longValue() > max + MESSAGES_PER_FILE) {
            indexesValid = false;
            LOGGER.error("Write index found invalid");
        }

        return indexesValid;
    }

    /**
     * This method checks if there are missing message data files.
     * @param dataFiles sorted message data file names in ascending order.
     * @return true if there is no missing message data file; false otherwise.
     */
    private boolean isMessageDataContinuous(String[] dataFiles) {
        if (null == dataFiles || 0 == dataFiles.length) {
            return true;
        }

        long[] dataFileLongArray = new long[dataFiles.length];
        boolean continuous = true;
        int i = 0;
        for (String dataFile: dataFiles) {
            dataFileLongArray[i] = Long.parseLong(dataFile);
            if (i > 0 && dataFileLongArray[i] != (dataFileLongArray[i - 1] + MESSAGES_PER_FILE)) {
                for (long j = dataFileLongArray[i - 1] + MESSAGES_PER_FILE; j < dataFileLongArray[i]; j += MESSAGES_PER_FILE) {
                    LOGGER.error("Found missing message data file:"+ localMessageStoreDirectory.getAbsolutePath()
                            + File.separator + String.valueOf(j));
                }
                continuous = false;
            }
            i++;
        }
        return continuous;
    }

    private void recoverWriteAheadData(File dataFile) throws IOException {
        checkFileToRead(dataFile);
        RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
        int recoveredMessageNumber = 0;
        try {
            while (recoveredMessageNumber++ < MESSAGES_PER_FILE) {

                if (writeOffSet.longValue() + 4 + 4 > randomAccessFile.length()) {
                    break;
                }

                int messageSize = randomAccessFile.readInt();
                int magicCode = randomAccessFile.readInt();
                if (magicCode != MessageEncoder.MAGIC_CODE) {
                    LOGGER.warn("Found an illegal magic code.");
                    break;
                }

                if (writeOffSet.longValue() + messageSize > randomAccessFile.length()) {
                    break;
                }

                byte[] messageData = new byte[messageSize - 4 - 4];
                randomAccessFile.readFully(messageData);
                ByteBuffer byteBuffer = ByteBuffer.allocate(messageSize);
                byteBuffer.putInt(messageSize);
                byteBuffer.putInt(magicCode);
                byteBuffer.put(messageData);
                byteBuffer.flip();
                MessageDecoder.decode(byteBuffer);
                writeIndex.incrementAndGet();
                if (writeIndex.longValue() % MESSAGES_PER_FILE == 0) {
                    writeOffSet.set(0);
                } else {
                    writeOffSet.addAndGet(messageSize);
                }
            }
        } finally {
            randomAccessFile.close();
            LOGGER.warn("Recovered " + recoveredMessageNumber + " messages.");
        }
    }

    /**
     * This method is synchronized as there may be multiple threads executing this thread.
     */
    private synchronized void updateConfig() {
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(configFile, false));
            bufferedWriter.write("writeIndex=" + writeIndex.longValue());
            bufferedWriter.newLine();
            bufferedWriter.write("writeOffSet=" + writeOffSet.longValue());
            bufferedWriter.newLine();
            bufferedWriter.write("readIndex=" + readIndex.longValue());
            bufferedWriter.newLine();
            bufferedWriter.write("readOffSet=" + readOffSet.longValue());
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            LOGGER.error("Unable to update config file", e);
        } finally {
            if (null != bufferedWriter) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    //ignore.
                }
            }
        }
        LOGGER.info("LocalMessageStore configuration file updated.");
    }

    /**
     * This method is assumed to execute concurrently.
     *
     * @param message Message to stash.
     */
    @Override
    public void stash(Message message) {
        if (ClientStatus.CLOSED == status || ClientStatus.CREATED == status) {
            throw new RuntimeException("Message store is not ready. You may have closed it already.");
        }

        try {
            messageQueue.put(wrap(message));

            //Check if we need flush some messages to disk.
            if (messageQueue.size() >= HIGH_QUEUE_LEVEL) {
                flushDiskService.putRequest(new FlushDiskRequest());
            }
        } catch (InterruptedException e) {
            LOGGER.error("Unable to stash message locally.", e);
            LOGGER.error("Fatal Error: Message [" + JSON.toJSONString(message) + "] is lost.");
        }
    }

    /**
     * Flush message into hark disk. If no sufficient usable disk space, no flush operation will be performed.
     */
    private void flush() {
        flush(false);
    }

    /**
     * Flush messages into hard disk.
     *
     * @param force Flush messages forcefully or not. true to flush all messages to disk, ignoring disk usage ratio
     *              warning.
     */
    private void flush(boolean force) {
        LOGGER.info("Local message store starts to flush.");

        if (!force) {
            float usableDiskSpaceRatio = getUsableDiskSpacePercent();
            if (usableDiskSpaceRatio < 1 - DISK_HIGH_WATER_LEVEL) {
                long current = System.currentTimeMillis();

                if (current - lastWarnTime > 2000 || -1 == lastWarnTime) {
                    LOGGER.error("No sufficient disk space! Cannot to flush!");
                    lastWarnTime = current;
                }

                if (current - lastFlushTime > 2000 || -1 == lastFlushTime) {
                    updateConfig();
                    lastFlushTime = current;
                }
                return;
            } else if (usableDiskSpaceRatio < 1 - DISK_WARNING_WATER_LEVEL) {
                long current = System.currentTimeMillis();
                if (current - lastWarnTime > 5000 || -1 == lastWarnTime) {
                    LOGGER.warn("Usable disk space now is only: " + usableDiskSpaceRatio + "%!");
                    lastWarnTime = current;
                }
            }
        }

        boolean isFlushingSuccessful = true;
        try {
            if (!lock.tryLock()) {
                lock.lockInterruptibly();
            }
            LOGGER.debug(Thread.currentThread().getName() + " holds the lock");
            Message message = messageQueue.poll();
            int numberOfMessageToCommit = 0;

            while (null != message) {
                long currentWriteIndex = writeIndex.longValue() + 1;
                if (1 == currentWriteIndex ||
                        (currentWriteIndex - 1) / MESSAGES_PER_FILE > (currentWriteIndex - 2) / MESSAGES_PER_FILE) {
                    //we need to create a new file.
                    File newMessageStoreFile = new File(localMessageStoreDirectory, String.valueOf(currentWriteIndex));
                    if (newMessageStoreFile.exists()) {
                        LOGGER.warn("IO Alarm!! File to create already exists! Its content will be overridden!");
                    } else if (!newMessageStoreFile.createNewFile()) {
                        throw new RuntimeException("Unable to create new local message store file");
                    }
                    checkFileToWrite(newMessageStoreFile);
                    messageStoreNameFileMapping.putIfAbsent(currentWriteIndex, newMessageStoreFile);

                    //close previous file.
                    if (null != writeRandomAccessFile) {
                        writeRandomAccessFile.close();
                    }
                    File dataFile = messageStoreNameFileMapping.get(currentWriteIndex);
                    writeRandomAccessFile = new RandomAccessFile(dataFile, ACCESS_FILE_MODE);
                }

                if (null == writeRandomAccessFile) {
                    File currentWritingDataFile = messageStoreNameFileMapping
                            .get(writeIndex.longValue() / MESSAGES_PER_FILE * MESSAGES_PER_FILE + 1);
                    checkFileToWrite(currentWritingDataFile);
                    writeRandomAccessFile = new RandomAccessFile(currentWritingDataFile, ACCESS_FILE_MODE);
                }
                byte[] msgData = MessageEncoder.encode(wrap(message)).array();
                writeRandomAccessFile.write(msgData);
                writeOffSet.addAndGet(msgData.length);
                writeIndex.incrementAndGet();
                if (writeIndex.longValue() % MESSAGES_PER_FILE == 0) {
                    writeOffSet.set(0L);
                }

                if (++numberOfMessageToCommit % UPDATE_CONFIG_PER_FLUSHING_NUMBER_OF_MESSAGE == 0) {
                    updateConfig();
                }

                if (!force) {
                    if (messageQueue.size() < LOW_QUEUE_LEVEL) {
                        break;
                    }
                }
                //Prepare for next round.
                message = messageQueue.poll();
            }

            updateConfig();
        } catch (Exception e) {
            isFlushingSuccessful = false;
            LOGGER.error("Flushing messages fails.", e);
        } finally {
            lock.unlock();
            LOGGER.debug(Thread.currentThread().getName() + " releases the lock.");

            if (isFlushingSuccessful) {
                LOGGER.info("Flushing messages completes successfully.");
            } else {
                LOGGER.warn("Flushing messages aborts due to error.");
            }
        }
    }

    private void checkFileToWrite(File file) throws IOException {
        if (null == file) {
            throw new IllegalArgumentException("File under writing is being null.");
        }

        if (!file.exists()) {
            throw new FileNotFoundException("File " + file.getAbsolutePath()
                    + " being written to is not found.");
        }

        if (!file.canWrite()) {
            throw new IOException("No write permission to " + file.getAbsolutePath());
        }
    }

    private void checkFileToRead(File file) throws IOException {
        if (null == file) {
            throw new IllegalArgumentException("File to read cannot be null");
        }

        if (!file.exists()) {
            throw new FileNotFoundException("File to read does not exist");
        }

        if (!file.canRead()) {
            throw new IOException("No read permission to " + file.getAbsolutePath());
        }
    }

    private float getUsableDiskSpacePercent() {
        try {
            FileStore fileStore = Files.getFileStore(localMessageStoreDirectory.toPath());
            return fileStore.getUsableSpace() * 1.0F / fileStore.getTotalSpace();
        } catch (IOException e) {
            LOGGER.error("Unable to get disk usage.", e);
            return 0.0F;
        }
    }

    @Override
    public MessageExt[] pop(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("n should be positive");
        }

        switch (status) {
            case CREATED:
                throw new RuntimeException("Message store is not ready. You may have closed it already.");
            default:
                break;
        }

        int messageToRead = Math.min(getNumberOfMessageStashed(), n);
        if (messageToRead < 1) {
            return null;
        }

        MessageExt[] messages = new MessageExt[messageToRead];
        int messageRead = 0;

        //First retrieve messages from message queue, beginning from head side, which is held in memory.
        MessageExt message = messageQueue.poll();
        while (null != message) {
            messages[messageRead++] = message;
            if (messageRead == messageToRead) { //We've already got all messages we want to pop.
                return messages;
            }
            message = messageQueue.poll();
        }

        //In case we need more messages, read from local files.
        RandomAccessFile readRandomAccessFile = null;
        //Popping messages from file requires lock.
        if (lock.tryLock()) {
            try {
                LOGGER.debug(Thread.currentThread().getName() + " holds the lock.");
                File currentReadFile = null;
                while (messageRead < messageToRead && readIndex.longValue() <= writeIndex.longValue()) {
                    if (null == readRandomAccessFile) {
                        currentReadFile = messageStoreNameFileMapping
                                .get(readIndex.longValue() / MESSAGES_PER_FILE * MESSAGES_PER_FILE + 1);
                        checkFileToRead(currentReadFile);
                        readRandomAccessFile = new RandomAccessFile(currentReadFile, ACCESS_FILE_MODE);
                        if (readOffSet.longValue() > 0) {
                            readRandomAccessFile.seek(readOffSet.longValue());
                        }
                    }

                    if (readOffSet.longValue() + 4 > readRandomAccessFile.length()) {
                        LOGGER.error("Data inconsistent!");
                        break;
                    }
                    int messageSize = readRandomAccessFile.readInt();

                    int magicCode = readRandomAccessFile.readInt();
                    if (magicCode != MessageEncoder.MAGIC_CODE) {
                        LOGGER.error("Data inconsistent!");
                    }

                    if (readOffSet.longValue() + messageSize > readRandomAccessFile.length()) {
                        LOGGER.error("Data inconsistent!");
                        break;
                    }

                    byte[] data = new byte[messageSize - 4 - 4];
                    readRandomAccessFile.readFully(data);

                    ByteBuffer byteBuffer = ByteBuffer.allocate(messageSize);
                    byteBuffer.putInt(messageSize);
                    byteBuffer.putInt(magicCode);
                    byteBuffer.put(data);
                    byteBuffer.flip();
                    messages[messageRead++] = MessageDecoder.decode(byteBuffer);
                    readIndex.incrementAndGet();
                    readOffSet.addAndGet(messageSize);

                    if (readIndex.longValue() % MESSAGES_PER_FILE == 0) {
                        readOffSet.set(0L);
                        readRandomAccessFile.close();
                        readRandomAccessFile = null;
                        messageStoreNameFileMapping.remove((readIndex.longValue() - 1) / MESSAGES_PER_FILE * MESSAGES_PER_FILE + 1);

                        if (currentReadFile.exists()) {
                            if (!currentReadFile.delete()) {
                                LOGGER.warn("Unable to delete used data file: {}", currentReadFile.getAbsolutePath());
                            }
                        }
                    }
                }
                updateConfig();
            } catch (Exception e) {
                LOGGER.error("Pop message fails.", e);
                LOGGER.error("readIndex:" + readIndex.longValue() + ", writeIndex:" + writeIndex.longValue()
                        + ", readOffset:" + readOffSet.longValue() + ", writeOffset:" + writeOffSet.longValue());
            } finally {
                if (null != readRandomAccessFile) {
                    try {
                        readRandomAccessFile.close();
                    } catch (IOException e) {
                        LOGGER.error("Unexpected IO error", e);
                    }
                }
                lock.unlock();
                LOGGER.debug(Thread.currentThread().getName() + " release the lock.");
            }
        }

        if (messageRead < 1) {
            return null;
        }

        if (messageRead < messageToRead) {
            MessageExt[] result = new MessageExt[messageRead];
            System.arraycopy(messages, 0, result, 0, messageRead);
            return result;
        } else {
            return messages;
        }
    }

    public int getNumberOfMessageStashed() {
        synchronized (DefaultLocalMessageStore.class) {
            return writeIndex.intValue() - readIndex.intValue() + messageQueue.size();
        }
    }

    public void close() throws InterruptedException {
        LOGGER.info("Default local message store starts to shut down.");
        status = ClientStatus.CLOSED;
        flushDiskService.shutdown();
        deleteAbortFile();
        LOGGER.info("Default local message store shuts down completely");
    }

    /**
     * All messages will forcefully flushed to disk.
     */
    public void suspend() {
        flushDiskService.putRequest(new FlushDiskRequest(true));
    }

    private MessageExt wrap(Message message) {
        if (message instanceof MessageExt) {
            return (MessageExt)message;
        }

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(message.getTopic());
        messageExt.setFlag(message.getFlag());
        messageExt.setBody(message.getBody());

        // Allow broker to decide.
        messageExt.setQueueId(-1);

        MessageAccessor.setProperties(messageExt, message.getProperties());

        messageExt.setBornHost(new InetSocketAddress(0));
        messageExt.setStoreHost(new InetSocketAddress(0));

        return messageExt;
    }
}
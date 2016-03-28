package com.alibaba.rocketmq.client.consumer.cacheable;

import com.alibaba.rocketmq.client.ClientStatus;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByDataCenter;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.store.LocalMessageStore;
import com.alibaba.rocketmq.client.store.MappedFileLocalMessageStore;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class CacheableConsumer {
    private static final Logger LOGGER = ClientLogger.getLog();

    private String consumerGroupName;

    protected LocalMessageStore localMessageStore;

    private final ConcurrentHashMap<String, MessageHandler> topicHandlerMap;

    private static final AtomicLong CONSUMER_NAME_COUNTER = new AtomicLong();

    private static final String BASE_INSTANCE_NAME = "CacheableConsumer";

    private static final int NUMBER_OF_CONSUMER = 4;

    private List<DefaultMQPushConsumer> defaultMQPushConsumers = new ArrayList<DefaultMQPushConsumer>();

    private volatile ClientStatus status = ClientStatus.CREATED;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    private static final int CORE_POOL_SIZE_FOR_WORK_TASKS = 10;

    private static final int MAXIMUM_POOL_SIZE_FOR_WORK_TASKS = 50;

    private static final int DEFAULT_PULL_BATCH_SIZE = 256;

    private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

    private static final int DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE = 1;

    private int consumeMessageMaxBatchSize = DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE;

    private int corePoolSizeForWorkTasks = CORE_POOL_SIZE_FOR_WORK_TASKS;

    private int maximumPoolSizeForWorkTasks = MAXIMUM_POOL_SIZE_FOR_WORK_TASKS;

    private ScheduledExecutorService scheduledExecutorDelayService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("LocalDelayConsumeService"));

    private ThreadPoolExecutor executorWorkerService;

    private FrontController frontController;

    private static final int DEFAULT_MAXIMUM_NUMBER_OF_MESSAGE_BUFFERED = 10000;

    private int maximumNumberOfMessageBuffered = DEFAULT_MAXIMUM_NUMBER_OF_MESSAGE_BUFFERED;

    private LinkedBlockingQueue<MessageExt> messageQueue;

    private final DelayService delayService;
    private final DelayQueue<DelayItem> delayQueue = new DelayQueue<>();

    private LinkedBlockingQueue<MessageExt> inProgressMessageQueue;

    private ScheduledExecutorService scheduledStatisticsReportExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StatisticsReportService"));

    private SynchronizedDescriptiveStatistics statistics;

    private volatile long previousSuccessCount = 0;

    private volatile long previousFailureCount = 0;

    private AtomicLong successCounter = new AtomicLong(0L);

    private AtomicLong failureCounter = new AtomicLong(0L);

    private static String getInstanceName() {
        return BASE_INSTANCE_NAME + "_" + CONSUMER_NAME_COUNTER.incrementAndGet();
    }

    /**
     * Constructor with group name and default number of embedded consumer clients.
     * @param consumerGroupName Consumer group name.
     */
    public CacheableConsumer(String consumerGroupName) {
        this(consumerGroupName, NUMBER_OF_CONSUMER);
    }

    /**
     * Constructor with consumer group name and specified number of embedded {@link DefaultMQPushConsumer} clients.
     * @param consumerGroupName consumer group name.
     * @param numberOfEmbeddedConsumers number of embedded consumer clients.
     */
    public CacheableConsumer(String consumerGroupName, int numberOfEmbeddedConsumers) {
        try {
            if (null == consumerGroupName || consumerGroupName.trim().isEmpty()) {
                throw new RuntimeException("ConsumerGroupName cannot be null or empty.");
            }

            this.consumerGroupName = consumerGroupName;
            this.topicHandlerMap = new ConcurrentHashMap<String, MessageHandler>();

            for (int i = 0; i < numberOfEmbeddedConsumers; i++) {
                DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroupName);

                // instance name should be set before getAndCreateMQClientInstance
                defaultMQPushConsumer.setInstanceName(getInstanceName());

                MQClientInstance clientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(defaultMQPushConsumer, null);
                defaultMQPushConsumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByDataCenter(clientInstance));
                defaultMQPushConsumer.setMessageModel(messageModel);
                defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
                defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
                defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);
                defaultMQPushConsumers.add(defaultMQPushConsumer);
            }

            executorWorkerService = new ThreadPoolExecutor(
                    corePoolSizeForWorkTasks,
                    maximumPoolSizeForWorkTasks,
                    0,
                    TimeUnit.NANOSECONDS,
                    new LinkedBlockingQueue<Runnable>(maximumPoolSizeForWorkTasks),
                    new ThreadFactoryImpl("ConsumerWorkerThread"),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            statistics = new SynchronizedDescriptiveStatistics(5000);
            localMessageStore = new MappedFileLocalMessageStore(consumerGroupName);
            frontController = new FrontController(this);
            delayService = new DelayService(this);

            scheduledStatisticsReportExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LOGGER.info("Business Processing Performance Simple Report:\n min {}ms,\n max {}ms,\n mean {}ms",
                            statistics.getMin(),
                            statistics.getMax(),
                            statistics.getMean());
                    LOGGER.info("Business Processing Performance Percentile Report: \n 5% {}ms,\n 10% {}ms,\n 20% {}ms," +
                                    "\n 40% {}ms,\n 50% {}ms,\n 80% {}ms,\n 90% {}ms,\n 95% {}ms,\n 100% {}ms",
                            statistics.getPercentile(5),
                            statistics.getPercentile(10),
                            statistics.getPercentile(20),
                            statistics.getPercentile(40),
                            statistics.getPercentile(50),
                            statistics.getPercentile(80),
                            statistics.getPercentile(90),
                            statistics.getPercentile(95),
                            statistics.getPercentile(100)
                    );
                    LOGGER.info("Number of messages pending to consume is: {}", messageQueue.size());
                    LOGGER.info("Number of messages under processing is: {}", inProgressMessageQueue.size());
                    LOGGER.info("Number of messages Stashed is: {}", localMessageStore.getNumberOfMessageStashed());

                    long currentSuccessCount = successCounter.get();
                    long currentFailureCount = failureCounter.get();
                    LOGGER.info("Success TPS: " + (currentSuccessCount - previousSuccessCount) / 30.0);
                    LOGGER.info("Failure TPS: " + (currentFailureCount - previousFailureCount) / 30.0);
                    previousSuccessCount = currentSuccessCount;
                    previousFailureCount = currentFailureCount;

                    LOGGER.info("Total number of successfully consumed messages: {}", successCounter.get());
                }
            }, 30, 30, TimeUnit.SECONDS);
        } catch (IOException e) {
            LOGGER.error("Fatal error. Possibly caused by File Permission Issue.", e);
            throw new RuntimeException("Fatal error while instantiating CacheableConsumer. Possibly caused by File Permission Issue.");
        }
    }

    public CacheableConsumer registerMessageHandler(MessageHandler messageHandler) throws MQClientException {
        if (status != ClientStatus.CREATED) {
            throw new IllegalStateException("Please register before start");
        }

        if (null == messageHandler.getTopic() || messageHandler.getTopic().trim().isEmpty()) {
            throw new RuntimeException("Topic cannot be null or empty");
        }

        topicHandlerMap.putIfAbsent(messageHandler.getTopic(), messageHandler);

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            defaultMQPushConsumer.subscribe(messageHandler.getTopic(),
                    null != messageHandler.getTag() ? messageHandler.getTag() : "*");
        }

        return this;
    }

    public CacheableConsumer registerMessageHandler(Collection<MessageHandler> messageHandlers)
            throws MQClientException {
        for (MessageHandler messageHandler : messageHandlers) {
            registerMessageHandler(messageHandler);
        }
        return this;
    }

    public void start() throws InterruptedException, MQClientException {
        if (topicHandlerMap.isEmpty()) {
            throw new RuntimeException("Please at least configure one message handler to subscribe one topic");
        }

        //We may have only one embedded consumer for broadcasting scenario.
        if (MessageModel.BROADCASTING == messageModel) {
            int i = 0;
            DefaultMQPushConsumer defaultMQPushConsumer = defaultMQPushConsumers.get(i);
            while (null == defaultMQPushConsumer && i < defaultMQPushConsumers.size()) {
                defaultMQPushConsumer = defaultMQPushConsumers.get(i++);
            }
            defaultMQPushConsumers.clear();
            defaultMQPushConsumers.add(defaultMQPushConsumer);
        }

        messageQueue = new LinkedBlockingQueue<MessageExt>(maximumNumberOfMessageBuffered);
        inProgressMessageQueue = new LinkedBlockingQueue<MessageExt>(maximumNumberOfMessageBuffered);

        frontController.startSubmittingJob();
        delayService.start();

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            defaultMQPushConsumer.registerMessageListener(frontController);
            defaultMQPushConsumer.start();
        }

        startPopThread();
        addShutdownHook();
        status = ClientStatus.ACTIVE;
        LOGGER.debug("DefaultMQPushConsumer starts.");
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Begin to shutdown CacheableConsumer");
                    shutdown();
                    LOGGER.info("CacheableConsumer shuts down successfully.");
                } catch (InterruptedException e) {
                    LOGGER.error("Exception thrown while invoking ShutdownHook", e);
                }
            }
        });
    }

    public int getMaximumNumberOfMessageBuffered() {
        return maximumNumberOfMessageBuffered;
    }

    public void setMaximumNumberOfMessageBuffered(int maximumNumberOfMessageBuffered) {
        this.maximumNumberOfMessageBuffered = maximumNumberOfMessageBuffered;
    }

    private void startPopThread() {
        PopStoreService popStoreService = new PopStoreService(this);
        scheduledExecutorDelayService.scheduleWithFixedDelay(popStoreService, 2, 2, TimeUnit.SECONDS);
    }

    public boolean isStarted() {
        return status == ClientStatus.ACTIVE;
    }

    public void setCorePoolSizeForWorkTasks(int corePoolSizeForWorkTasks) {
        this.corePoolSizeForWorkTasks = corePoolSizeForWorkTasks;
    }

    public void setMaximumPoolSizeForWorkTasks(int maximumPoolSizeForWorkTasks) {
        this.maximumPoolSizeForWorkTasks = maximumPoolSizeForWorkTasks;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set message model before start");
        }

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            if (null != defaultMQPushConsumer) {
                defaultMQPushConsumer.setMessageModel(messageModel);
            }
        }
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consume-from-where before start");
        }

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            if (null != defaultMQPushConsumer) {
                defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
            }
        }
    }

    public void setConsumeMessageMaxBatchSize(int consumeMessageMaxBatchSize) {
        this.consumeMessageMaxBatchSize = consumeMessageMaxBatchSize;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consumeMessageMaxBatchSize before start");
        }

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            if (null != defaultMQPushConsumer) {
                defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);
            }
        }
    }

    public int getConsumeMessageMaxBatchSize() {
        return consumeMessageMaxBatchSize;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set pullBatchSize before start");
        }

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            if (null != defaultMQPushConsumer) {
                defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
            }
        }
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public LocalMessageStore getLocalMessageStore() {
        return localMessageStore;
    }

    public LinkedBlockingQueue<MessageExt> getMessageQueue() {
        return messageQueue;
    }

    public LinkedBlockingQueue<MessageExt> getInProgressMessageQueue() {
        return inProgressMessageQueue;
    }

    public SynchronizedDescriptiveStatistics getStatistics() {
        return statistics;
    }

    public ConcurrentHashMap<String, MessageHandler> getTopicHandlerMap() {
        return topicHandlerMap;
    }

    public ThreadPoolExecutor getExecutorWorkerService() {
        return executorWorkerService;
    }

    public AtomicLong getSuccessCounter() {
        return successCounter;
    }

    public AtomicLong getFailureCounter() {
        return failureCounter;
    }

    /**
     * This method shuts down this client properly.
     * @throws InterruptedException If unable to shut down within 1 minute.
     */
    public void shutdown() throws InterruptedException {
        LOGGER.info("Start to shutdown");
        try {
            stopReceiving();
        } catch (InterruptedException e) {
            LOGGER.error("Failed to stop", e);
        }

        try {
            //Shut down local message store.
            if (null != localMessageStore) {
                localMessageStore.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failed to stop", e);
        }
        LOGGER.info("Shutdown completes");
    }

    protected void stopReceiving() throws InterruptedException {
        if (status == ClientStatus.ACTIVE || status == ClientStatus.SUSPENDED) {
            //Stop pulling messages from broker server.
            for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
                defaultMQPushConsumer.shutdown();
            }

            //Stop popping messages from local message store.
            scheduledExecutorDelayService.shutdown();
            scheduledExecutorDelayService.awaitTermination(30000, TimeUnit.MILLISECONDS);

            frontController.stopSubmittingJob();

            //Stop consuming messages.
            executorWorkerService.shutdown();
            executorWorkerService.awaitTermination(30000, TimeUnit.MILLISECONDS);


            //Stash back all those that is not properly handled.
            LOGGER.info(messageQueue.size() + " messages to save into local message store due to system shutdown.");
            stashBlockingMessageQueue(messageQueue);
            stashBlockingMessageQueue(inProgressMessageQueue);

            delayService.shutdown();
            for (DelayItem item : delayQueue) {
                localMessageStore.stash(item.getMessage());
            }

            status = ClientStatus.CLOSED;
            LOGGER.info("Local messages saving completes.");
        }
    }

    private void stashBlockingMessageQueue(LinkedBlockingQueue<MessageExt> messageQueue) {
        if (null == messageQueue || messageQueue.isEmpty()) {
            return;
        }

        if (messageQueue.size() > 0) {
            MessageExt messageExt = messageQueue.poll();
            while (null != messageExt) {
                localMessageStore.stash(messageExt);
                messageExt = messageQueue.poll();
            }
        }
    }

    public void suspend() {
        if (ClientStatus.SUSPENDED == status) {
            return;
        }

        if (ClientStatus.ACTIVE == status) {
            for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
                defaultMQPushConsumer.suspend();
            }
            status = ClientStatus.SUSPENDED;
        }
    }

    public void resume() {
        if (ClientStatus.SUSPENDED == status) {
            LOGGER.info("Start to resume consumer client");
            for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
                defaultMQPushConsumer.resume();
            }
            status = ClientStatus.ACTIVE;
            LOGGER.info("Consumer client resumed.");
        }
    }

    public DelayQueue<DelayItem> getDelayQueue() {
        return delayQueue;
    }

    public int getMaximumPoolSizeForWorkTasks() {
        return maximumPoolSizeForWorkTasks;
    }

    public ClientStatus getStatus() {
        return status;
    }

    public boolean isAboutFull() {
       return messageQueue.remainingCapacity() <= 2 * maximumPoolSizeForWorkTasks + inProgressMessageQueue.size() + DEFAULT_PULL_BATCH_SIZE;
    }

    public boolean mayResume() {
        return messageQueue.remainingCapacity() >=  maximumNumberOfMessageBuffered * 2 / 3;
    }
}

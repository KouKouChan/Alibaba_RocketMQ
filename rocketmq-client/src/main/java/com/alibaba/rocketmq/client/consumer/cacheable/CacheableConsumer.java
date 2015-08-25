package com.alibaba.rocketmq.client.consumer.cacheable;

import com.alibaba.rocketmq.client.ClientStatus;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByDataCenter;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.concurrent.DefaultLocalMessageStore;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CacheableConsumer {
    private static final Logger LOGGER = ClientLogger.getLog();

    private String consumerGroupName;

    protected DefaultLocalMessageStore localMessageStore;

    private final ConcurrentHashMap<String, MessageHandler> topicHandlerMap;

    private DefaultMQPushConsumer defaultMQPushConsumer;

    private ClientStatus status = ClientStatus.CREATED;

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

    private static final int DEFAULT_MAXIMUM_NUMBER_OF_MESSAGE_BUFFERED = 20000;

    private int maximumNumberOfMessageBuffered = DEFAULT_MAXIMUM_NUMBER_OF_MESSAGE_BUFFERED;

    private LinkedBlockingQueue<MessageExt> messageQueue;

    private LinkedBlockingQueue<MessageExt> inProgressMessageQueue;

    private ScheduledExecutorService scheduledStatisticsReportExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StatisticsReportService"));

    private SynchronizedDescriptiveStatistics statistics;

    private volatile long previousSuccessCount = 0;

    private volatile long previousFailureCount = 0;

    private AtomicLong successCounter = new AtomicLong(0L);

    private AtomicLong failureCounter = new AtomicLong(0L);

    /**
     * Constructor with consumer group name and specified number of embedded {@link DefaultMQPushConsumer} clients.
     * @param consumerGroupName consumer group name.
     */
    public CacheableConsumer(String consumerGroupName) {
        try {
            if (null == consumerGroupName || consumerGroupName.trim().isEmpty()) {
                throw new RuntimeException("ConsumerGroupName cannot be null or empty.");
            }

            this.consumerGroupName = consumerGroupName;
            this.topicHandlerMap = new ConcurrentHashMap<String, MessageHandler>();
            this.defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroupName);
            defaultMQPushConsumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByDataCenter(defaultMQPushConsumer));
            defaultMQPushConsumer.setMessageModel(messageModel);
            defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
            defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
            defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);

            executorWorkerService = new ThreadPoolExecutor(
                    corePoolSizeForWorkTasks,
                    maximumPoolSizeForWorkTasks,
                    0,
                    TimeUnit.NANOSECONDS,
                    new LinkedBlockingQueue<Runnable>(maximumPoolSizeForWorkTasks),
                    new ThreadFactoryImpl("ConsumerWorkerThread"),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            statistics = new SynchronizedDescriptiveStatistics(5000);
            localMessageStore = new DefaultLocalMessageStore(consumerGroupName);
            frontController = new FrontController(this);
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

        defaultMQPushConsumer.subscribe(messageHandler.getTopic(),
                null != messageHandler.getTag() ? messageHandler.getTag() : "*");
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

        messageQueue = new LinkedBlockingQueue<MessageExt>(maximumNumberOfMessageBuffered);
        inProgressMessageQueue = new LinkedBlockingQueue<MessageExt>(maximumNumberOfMessageBuffered);
        defaultMQPushConsumer.registerMessageListener(frontController);
        defaultMQPushConsumer.start();
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
        DelayTask delayTask = new DelayTask(this);
        scheduledExecutorDelayService.scheduleWithFixedDelay(delayTask, 2, 2, TimeUnit.SECONDS);
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

        defaultMQPushConsumer.setMessageModel(messageModel);
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consume-from-where before start");
        }
        defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
    }

    public void setConsumeMessageMaxBatchSize(int consumeMessageMaxBatchSize) {
        this.consumeMessageMaxBatchSize = consumeMessageMaxBatchSize;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consumeMessageMaxBatchSize before start");
        }
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);
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
        defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public DefaultLocalMessageStore getLocalMessageStore() {
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
        status = ClientStatus.CLOSED;
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
            defaultMQPushConsumer.shutdown();

            //Stop popping messages from local message store.
            scheduledExecutorDelayService.shutdown();
            scheduledExecutorDelayService.awaitTermination(30000, TimeUnit.MILLISECONDS);

            //Stop consuming messages.
            executorWorkerService.shutdown();
            executorWorkerService.awaitTermination(30000, TimeUnit.MILLISECONDS);

            frontController.stopSubmittingJob();

            //Stash back all those that is not properly handled.
            LOGGER.info(messageQueue.size() + " messages to save into local message store due to system shutdown.");
            if (messageQueue.size() > 0) {
                MessageExt messageExt = messageQueue.poll();
                while (null != messageExt) {
                    localMessageStore.stash(messageExt);
                    messageExt = messageQueue.poll();
                }
            }

            if (inProgressMessageQueue.size() > 0) {
                MessageExt messageExt = inProgressMessageQueue.poll();
                while (null != messageExt) {
                    localMessageStore.stash(messageExt);
                    messageExt = inProgressMessageQueue.poll();
                }
            }

            status = ClientStatus.CLOSED;
            LOGGER.info("Local messages saving completes.");
        }
    }

    public void suspend() {
        if (ClientStatus.SUSPENDED == status) {
            return;
        }

        if (ClientStatus.ACTIVE == status) {
            defaultMQPushConsumer.suspend();
            localMessageStore.suspend();
            status = ClientStatus.SUSPENDED;
        }
    }

    public void resume() {
        if (ClientStatus.SUSPENDED == status) {
            status = ClientStatus.ACTIVE;
            localMessageStore.resume();
            defaultMQPushConsumer.resume();
        }
    }
}

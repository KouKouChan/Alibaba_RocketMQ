package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.client.ClientStatus;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByDataCenter;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.store.DefaultLocalMessageStore;
import com.alibaba.rocketmq.client.store.LocalMessageStore;
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

public class BufferedMQConsumer {

    private static final Logger LOGGER = ClientLogger.getLog();

    private final ConcurrentHashMap<String, MessageHandler> topicHandlerMap;

    private DefaultMQPushConsumer defaultMQPushConsumer;

    private volatile ClientStatus status = ClientStatus.CREATED;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    private static final int DEFAULT_PULL_BATCH_SIZE = 1024;

    private static final int DEFAULT_PULL_THRESHOLD = 4096;

    private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

    private int pullThreshold = DEFAULT_PULL_THRESHOLD;

    private static final int DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE = 1;

    private int consumeMessageMaxBatchSize = DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE;

    private FrontController frontController;

    private SynchronizedDescriptiveStatistics statistics;

    private volatile long previousSuccessCount = 0;

    private AtomicLong successCounter = new AtomicLong(0L);

    /**
     * Constructor with consumer group name and specified number of embedded {@link DefaultMQPushConsumer} clients.
     * @param consumerGroupName consumer group name.
     */
    public BufferedMQConsumer(final String consumerGroupName) {
        if (null == consumerGroupName || consumerGroupName.trim().isEmpty()) {
            throw new RuntimeException("ConsumerGroupName cannot be null or empty.");
        }

        this.topicHandlerMap = new ConcurrentHashMap<>();
        defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroupName);
        defaultMQPushConsumer.changeInstanceNameToPID();
        MQClientInstance clientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(defaultMQPushConsumer, null);

        defaultMQPushConsumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByDataCenter(clientInstance));
        defaultMQPushConsumer.setMessageModel(messageModel);
        defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
        defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);

        statistics = new SynchronizedDescriptiveStatistics(5000);
        frontController = new FrontController(this);

        ScheduledExecutorService scheduledStatisticsReportExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StatisticsReportService"));
        scheduledStatisticsReportExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Business Processing Performance Simple Report: \nConsumer Group: {} \n min {}ms,\n max {}ms,\n mean {}ms",
                            consumerGroupName,
                            statistics.getMin(),
                            statistics.getMax(),
                            statistics.getMean());

                    LOGGER.info("Business Processing Performance Percentile Report: \nConsumer Group:{} \n 5% {}ms,\n 10% {}ms,\n 20% {}ms," +
                                    "\n 40% {}ms,\n 50% {}ms,\n 80% {}ms,\n 90% {}ms,\n 95% {}ms,\n 100% {}ms",
                            consumerGroupName,
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
                    statistics.clear();

                    long currentSuccessCount = successCounter.get();
                    LOGGER.info("Success TPS: " + (currentSuccessCount - previousSuccessCount) / 30.0);
                    previousSuccessCount = currentSuccessCount;

                    LOGGER.info("Total number of successfully consumed messages: {}", successCounter.get());
                } catch (Exception e) {
                    LOGGER.error("Unexpected error while reporting statistics", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public BufferedMQConsumer registerMessageHandler(MessageHandler messageHandler) throws MQClientException {
        if (status != ClientStatus.CREATED) {
            throw new IllegalStateException("Please register before start");
        }

        if (null == messageHandler.getTopic() || messageHandler.getTopic().trim().isEmpty()) {
            throw new RuntimeException("Topic cannot be null or empty");
        }

        topicHandlerMap.putIfAbsent(messageHandler.getTopic(), messageHandler);
        defaultMQPushConsumer.subscribe(messageHandler.getTopic(), null != messageHandler.getTag() ? messageHandler.getTag() : "*");
        return this;
    }

    public BufferedMQConsumer registerMessageHandler(Collection<MessageHandler> messageHandlers)
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

        defaultMQPushConsumer.registerMessageListener(frontController);
        defaultMQPushConsumer.start();
        addShutdownHook();
        status = ClientStatus.ACTIVE;
        LOGGER.debug("DefaultMQPushConsumer starts.");
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Begin to shutdown BufferedMQConsumer");
                    shutdown();
                    LOGGER.info("BufferedMQConsumer shuts down successfully.");
                } catch (InterruptedException e) {
                    LOGGER.error("Exception thrown while invoking ShutdownHook", e);
                }
            }
        });
    }

    public boolean isStarted() {
        return status == ClientStatus.ACTIVE;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set message model before start");
        }

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.setMessageModel(messageModel);
        }
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consume-from-where before start");
        }

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
        }
    }

    public void setConsumeMessageMaxBatchSize(int consumeMessageMaxBatchSize) {

        this.consumeMessageMaxBatchSize = consumeMessageMaxBatchSize;

        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set consumeMessageMaxBatchSize before start");
        }

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.setConsumeMessageBatchMaxSize(consumeMessageMaxBatchSize);
        }
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set pullBatchSize before start");
        }

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
        }
    }

    public void pullThresholdForQueue(int pullThreshold) {
        this.pullThreshold = pullThreshold;
        if (status != ClientStatus.CREATED) {
            throw new RuntimeException("Please set pullThreshold before start");
        }

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.setPullThresholdForQueue(pullThreshold);
        }
    }

    public SynchronizedDescriptiveStatistics getStatistics() {
        return statistics;
    }

    public ConcurrentHashMap<String, MessageHandler> getTopicHandlerMap() {
        return topicHandlerMap;
    }


    public AtomicLong getSuccessCounter() {
        return successCounter;
    }

    /**
     * This method shuts down this client properly.
     * @throws InterruptedException If unable to shut down within 1 minute.
     */
    public void shutdown() throws InterruptedException {
        LOGGER.info("Start to shutdown");

        if (null != defaultMQPushConsumer) {
            defaultMQPushConsumer.shutdown();
        }

        LOGGER.info("Shutdown completes");
    }

    public ClientStatus getStatus() {
        return status;
    }
}

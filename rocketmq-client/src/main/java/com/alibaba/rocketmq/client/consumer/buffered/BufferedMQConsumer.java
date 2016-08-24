package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.client.ClientStatus;
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

    private static final AtomicLong CONSUMER_NAME_COUNTER = new AtomicLong();

    private static final String BASE_INSTANCE_NAME = "BufferedMQConsumer";

    private static final int NUMBER_OF_CONSUMER = 4;

    private List<DefaultMQPushConsumer> defaultMQPushConsumers = new ArrayList<DefaultMQPushConsumer>();

    private volatile ClientStatus status = ClientStatus.CREATED;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    private static final int DEFAULT_PULL_BATCH_SIZE = 32;

    private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

    private static final int DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE = 1;

    private int consumeMessageMaxBatchSize = DEFAULT_CONSUME_MESSAGE_MAX_BATCH_SIZE;

    private FrontController frontController;

    private SynchronizedDescriptiveStatistics statistics;

    private volatile long previousSuccessCount = 0;

    private AtomicLong successCounter = new AtomicLong(0L);

    private static String getInstanceName() {
        return BASE_INSTANCE_NAME + "_" + CONSUMER_NAME_COUNTER.incrementAndGet();
    }

    /**
     * Constructor with group name and default number of embedded consumer clients.
     * @param consumerGroupName Consumer group name.
     */
    public BufferedMQConsumer(String consumerGroupName) {
        this(consumerGroupName, NUMBER_OF_CONSUMER);
    }

    /**
     * Constructor with consumer group name and specified number of embedded {@link DefaultMQPushConsumer} clients.
     * @param consumerGroupName consumer group name.
     * @param numberOfEmbeddedConsumers number of embedded consumer clients.
     */
    public BufferedMQConsumer(String consumerGroupName, int numberOfEmbeddedConsumers) {
        if (null == consumerGroupName || consumerGroupName.trim().isEmpty()) {
            throw new RuntimeException("ConsumerGroupName cannot be null or empty.");
        }

        this.topicHandlerMap = new ConcurrentHashMap<>();

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

        statistics = new SynchronizedDescriptiveStatistics(5000);
        frontController = new FrontController(this);

        ScheduledExecutorService scheduledStatisticsReportExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StatisticsReportService"));
        scheduledStatisticsReportExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {

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

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            defaultMQPushConsumer.subscribe(messageHandler.getTopic(),
                    null != messageHandler.getTag() ? messageHandler.getTag() : "*");
        }

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

        for (DefaultMQPushConsumer defaultMQPushConsumer : defaultMQPushConsumers) {
            defaultMQPushConsumer.registerMessageListener(frontController);
            defaultMQPushConsumer.start();
        }

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

        for (DefaultMQPushConsumer consumer : defaultMQPushConsumers) {
            consumer.shutdown();
        }

        LOGGER.info("Shutdown completes");
    }

    public ClientStatus getStatus() {
        return status;
    }
}

package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SelectMessageQueueByAccumulation implements MessageQueueSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectMessageQueueByAccumulation.class);

    private final DefaultMQProducer producer;

    private final Map<String/* Topic */, String/* Consumer Group */> topicSubscription;

    private final ConcurrentHashMap<MessageQueue, Long> accumulation;

    private final ScheduledExecutorService executorService;

    public SelectMessageQueueByAccumulation(DefaultMQProducer producer, Map<String, String> topicSubscription) {
        this.producer = producer;
        this.topicSubscription = topicSubscription;
        this.accumulation = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();

    }

    public void start() {
        executorService.scheduleWithFixedDelay(new RefreshAccumulationTask(), 0, 300, TimeUnit.SECONDS);
        LOGGER.info("SelectMessageQueueByAccumulation starts");
    }

    public void shutdown() {
        executorService.shutdown();
        LOGGER.info("SelectMessageQueueByAccumulation stops");
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {

        return null;
    }

    private void refreshAccumulation() {
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(producer, null);
        for (Map.Entry<String, String> next : topicSubscription.entrySet()) {
            String topic = next.getKey();
            String consumeGroup = next.getValue();
            try {
                OffsetStore offsetStore = new RemoteBrokerOffsetStore(mqClientInstance, consumeGroup);
                List<MessageQueue> messageQueueList = producer.fetchPublishMessageQueues(topic);
                if (messageQueueList != null && !messageQueueList.isEmpty()) {
                    for (MessageQueue messageQueue : messageQueueList) {
                        long max = producer.maxOffset(messageQueue);
                        long consumeOffset = offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
                        if (consumeOffset < 0) {
                            consumeOffset = 0;
                        }
                        accumulation.put(messageQueue, max - consumeOffset);
                        LOGGER.info("Message Queue: {}, accumulation: {}", messageQueue, accumulation.get(messageQueue));
                    }
                }
            } catch (MQClientException e) {
                LOGGER.error("Failed to fetch accumulation data. Topic: {}, Consumer Group: {}", topic, consumeGroup);
            }
        }
    }

    private class RefreshAccumulationTask implements Runnable {
        @Override
        public void run() {
            LOGGER.info("Begin to refresh accumulation");
            try {
                refreshAccumulation();
                LOGGER.info("Refresh accumulation finished");
            } catch (Exception e) {
                LOGGER.error("Refresh accumulation failed", e);
            }
        }
    }
}

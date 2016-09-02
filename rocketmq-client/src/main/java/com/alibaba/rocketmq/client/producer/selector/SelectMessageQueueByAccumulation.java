package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.Pair;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    private final ConcurrentHashMap<List<MessageQueue>, List<Pair<MessageQueue, Long>>> weightCache;
    private final Random random;

    public SelectMessageQueueByAccumulation(DefaultMQProducer producer, Map<String, String> topicSubscription) {
        this.producer = producer;
        this.topicSubscription = topicSubscription;
        this.accumulation = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        weightCache = new ConcurrentHashMap<>();
        random = new Random(System.currentTimeMillis());
    }

    public void start() {
        executorService.scheduleWithFixedDelay(new RefreshAccumulationTask(), 0, 300, TimeUnit.SECONDS);
        LOGGER.info("SelectMessageQueueByAccumulation starts");
    }

    public void shutdown() {
        executorService.shutdown();
        LOGGER.info("SelectMessageQueueByAccumulation stops");
    }

    private void computeWeight(List<MessageQueue> mqs) {
        int len = mqs.size();

        List<Pair<MessageQueue, Long>> list = new ArrayList<>(len);
        for (MessageQueue messageQueue : mqs) {
            Pair<MessageQueue, Long> pair = new Pair<>(messageQueue, accumulation.containsKey(messageQueue) ? accumulation.get(messageQueue) : 0L);
            list.add(pair);
        }

        Collections.sort(list, new Comparator<Pair<MessageQueue, Long>>() {
            @Override
            public int compare(Pair<MessageQueue, Long> lhs, Pair<MessageQueue, Long> rhs) {
                return Long.compare(lhs.getObject2(), rhs.getObject2());
            }
        });


        for (int i = 0; i < len / 2; i++) {
            Pair<MessageQueue, Long> head = list.get(i);
            Pair<MessageQueue, Long> tail = list.get(len - 1 - i);
            long temp = head.getObject2();
            head.setObject2(tail.getObject2());
            tail.setObject2(temp);
        }

        for (int i = 1; i < len; i++) {
            Pair<MessageQueue, Long> prev = list.get(i - 1);
            Pair<MessageQueue, Long> next = list.get(i);
            next.setObject2(next.getObject2() + prev.getObject2());
        }

        weightCache.put(mqs, list);
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {

        if (!weightCache.containsKey(mqs)) {
            computeWeight(mqs);
        }

        int len = mqs.size();
        List<Pair<MessageQueue, Long>> list = weightCache.get(mqs);

        int n = random.nextInt(list.get(len - 1).getObject2().intValue());

        for (int i = 0; i < len; i++) {
            Pair<MessageQueue, Long> pair = list.get(i);
            if (pair.getObject2() > n) {
                return pair.getObject1();
            }
        }

        return list.get(len - 1).getObject1();
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

        // Clear previous cached weight
        weightCache.clear();
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

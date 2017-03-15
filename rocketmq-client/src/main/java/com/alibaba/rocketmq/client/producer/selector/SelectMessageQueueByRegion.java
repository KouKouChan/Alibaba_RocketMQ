package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SelectMessageQueueByRegion implements MessageQueueSelector {

    private Region target;

    private final AtomicReference<List<MessageQueue>> targetMessageQueues = new AtomicReference<>();

    private final AtomicReference<List<MessageQueue>> messageQueues = new AtomicReference<>();

    private final AtomicLong cursor = new AtomicLong();

    public SelectMessageQueueByRegion(Region target) {
        if (null == target) {
            this.target = Region.SAME;
        } else {
            this.target = target;
        }

        if (Region.SAME == this.target) {
            this.target = Region.parse(Integer.parseInt(Util.LOCAL_DATA_CENTER_ID));
        }

        schedule();
    }

    private void schedule() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "SelectMessageQueueByRegion");
                thread.setDaemon(true);
                return thread;
            }
        });

        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                update();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void update() {
        if (Region.ANY == this.target) {
            targetMessageQueues.set(messageQueues.get());
            return;
        }

        List<MessageQueue> all = messageQueues.get();
        List<MessageQueue> result = new ArrayList<>();
        for (MessageQueue messageQueue : all) {
            if (Util.getRegionIndex(messageQueue) == target.getIndex()) {
                result.add(messageQueue);
            }
        }

        if (!result.isEmpty()) {
            targetMessageQueues.set(result);
        } else {
            targetMessageQueues.set(all);
        }
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        messageQueues.set(mqs);

        if (null == targetMessageQueues.get()) {
            update();
        }

        List<MessageQueue> messageQueues = targetMessageQueues.get();

        return messageQueues.get((int)(cursor.getAndIncrement() % messageQueues.size()));
    }
}

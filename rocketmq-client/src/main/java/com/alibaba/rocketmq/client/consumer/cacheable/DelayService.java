package com.alibaba.rocketmq.client.consumer.cacheable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayService implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayService.class);

    private final CacheableConsumer cacheableConsumer;

    private volatile boolean stopped;

    private final Thread thread;

    public DelayService(CacheableConsumer cacheableConsumer) {
        this.cacheableConsumer = cacheableConsumer;
        thread = new Thread(this);
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                DelayItem delayItem = cacheableConsumer.getDelayQueue().take();
                if (!cacheableConsumer.getMessageQueue().offer(delayItem.getMessage())) {
                    cacheableConsumer.getLocalMessageStore().stash(delayItem.getMessage());
                }
            } catch (InterruptedException e) {
                if (stopped) {
                    break;
                } else {
                    LOGGER.warn("Thread got interrupted");
                }
            }
        }
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        stopped = true;

        thread.interrupt();
    }
}

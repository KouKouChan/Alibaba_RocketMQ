package com.alibaba.rocketmq.client.consumer.buffered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayService implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayService.class);

    private final BufferedMQConsumer bufferedMQConsumer;

    private volatile boolean stopped;

    private final Thread thread;

    public DelayService(BufferedMQConsumer bufferedMQConsumer) {
        this.bufferedMQConsumer = bufferedMQConsumer;
        thread = new Thread(this);
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                DelayItem delayItem = bufferedMQConsumer.getDelayQueue().take();
                if (!bufferedMQConsumer.getMessageQueue().offer(delayItem.getMessage())) {
                    bufferedMQConsumer.getLocalMessageStore().stash(delayItem.getMessage());
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

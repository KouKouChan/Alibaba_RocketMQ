package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.client.ClientStatus;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;

public class ProcessMessageTask implements Runnable {

    private static final Logger LOGGER = ClientLogger.getLog();

    private MessageExt message;

    private MessageHandler messageHandler;

    private BufferedMQConsumer bufferedMQConsumer;

    public ProcessMessageTask(MessageExt message, //Message to process.
                              MessageHandler messageHandler, //Message handler.
                              BufferedMQConsumer bufferedMQConsumer
    ) {
        this.message = message;
        this.messageHandler = messageHandler;
        this.bufferedMQConsumer = bufferedMQConsumer;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            int result = messageHandler.handle(message);
            if (0 == result) {
                bufferedMQConsumer.getStatistics().addValue(System.currentTimeMillis() - start);
                bufferedMQConsumer.getSuccessCounter().incrementAndGet();
            } else if (result > 0) {
                bufferedMQConsumer.getFailureCounter().incrementAndGet();
                DelayItem delayItem = new DelayItem(message, result);
                bufferedMQConsumer.getDelayQueue().offer(delayItem);
            } else {
                LOGGER.error("Unable to process returning result: " + result);
            }
        } catch (Exception e) {
            bufferedMQConsumer.getLocalMessageStore().stash(message);
            LOGGER.error("Yuck! Business processing logic is buggy; Stash the message for now.");
            LOGGER.error("ProcessMessageTask failed! Automatic retry scheduled.", e);
        } finally {
            bufferedMQConsumer.getInProgressMessageQueue().remove(message);
        }

        try {
            if (bufferedMQConsumer.getStatus() == ClientStatus.SUSPENDED && bufferedMQConsumer.mayResume()) {
                bufferedMQConsumer.resume();
            }
        } catch (Throwable e) {
            LOGGER.error("Error to resume consumer client", e);
        }
    }

}

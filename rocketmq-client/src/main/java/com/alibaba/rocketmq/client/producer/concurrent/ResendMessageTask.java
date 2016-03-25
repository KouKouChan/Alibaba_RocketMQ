package com.alibaba.rocketmq.client.producer.concurrent;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.store.LocalMessageStore;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;

public class ResendMessageTask implements Runnable {

    /**
     * Indicate number of messages to retrieve from local message store each time.
     */
    private static final int BATCH_FETCH_MESSAGE_FROM_STORE_SIZE = 100;

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = ClientLogger.getLog();

    private LocalMessageStore localMessageStore;

    private MultiThreadMQProducer multiThreadMQProducer;

    public ResendMessageTask(LocalMessageStore localMessageStore, MultiThreadMQProducer multiThreadMQProducer) {
        this.localMessageStore = localMessageStore;
        this.multiThreadMQProducer = multiThreadMQProducer;
    }

    @Override
    public void run() {
        try {
            LOGGER.debug("Start to re-send");
            if (localMessageStore.getNumberOfMessageStashed() == 0) {
                LOGGER.debug("No stashed messages to re-send");
                return;
            }

            int popSize = Math.min(multiThreadMQProducer.getSemaphore().availablePermits(),
                    BATCH_FETCH_MESSAGE_FROM_STORE_SIZE);

            if (popSize <= 0) {
                LOGGER.debug("No permits available in semaphore. Yield and wait for next round.");
                return;
            }

            Message[] messages = localMessageStore.pop(popSize);
            if (null == messages || messages.length == 0) {
                LOGGER.debug("No stashed messages to re-send");
                return;
            }

            int totalNumberOfMessagesSubmitted = 0;

            while (null != messages && messages.length > 0) {
                multiThreadMQProducer.send(messages);
                totalNumberOfMessagesSubmitted += messages.length;
                //Prepare for next loop step.
                popSize = Math.min(multiThreadMQProducer.getSemaphore().availablePermits(),
                        BATCH_FETCH_MESSAGE_FROM_STORE_SIZE);
                if (popSize <= 0) {
                    break;
                }
                messages = localMessageStore.pop(popSize);
            }

            LOGGER.debug(totalNumberOfMessagesSubmitted + " stashed messages re-sending completes: scheduled job submitted.");
        } catch (Exception e) {
            LOGGER.error("ResendMessageTask got an exception!", e);
        }

    }
}

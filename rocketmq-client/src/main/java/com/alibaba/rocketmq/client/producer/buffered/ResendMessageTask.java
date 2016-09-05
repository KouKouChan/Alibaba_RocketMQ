package com.alibaba.rocketmq.client.producer.buffered;

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

    private BufferedMQProducer bufferedMQProducer;

    public ResendMessageTask(LocalMessageStore localMessageStore, BufferedMQProducer bufferedMQProducer) {
        this.localMessageStore = localMessageStore;
        this.bufferedMQProducer = bufferedMQProducer;
    }

    @Override
    public void run() {
        try {
            LOGGER.debug("Start to re-send");
            if (localMessageStore.getNumberOfMessageStashed() == 0) {
                LOGGER.debug("No stashed messages to re-send");
                return;
            }

            Message[] messages = localMessageStore.pop(BATCH_FETCH_MESSAGE_FROM_STORE_SIZE);
            if (null == messages || messages.length == 0) {
                LOGGER.debug("No stashed messages to re-send");
                return;
            }

            int totalNumberOfMessagesSubmitted = 0;
            while (null != messages && messages.length > 0) {
                bufferedMQProducer.send(messages);
                totalNumberOfMessagesSubmitted += messages.length;
                messages = localMessageStore.pop(BATCH_FETCH_MESSAGE_FROM_STORE_SIZE);
            }

            LOGGER.debug(totalNumberOfMessagesSubmitted + " stashed messages re-sending completes: scheduled job submitted.");
        } catch (Exception e) {
            LOGGER.error("ResendMessageTask got an exception!", e);
        }

    }
}

package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;

public class PopMessageService implements Runnable {

    private static final Logger LOGGER = ClientLogger.getLog();

    private static final int BATCH_SIZE = 1000;

    private BufferedMQConsumer bufferedMQConsumer;

    public PopMessageService(BufferedMQConsumer bufferedMQConsumer) {
        this.bufferedMQConsumer = bufferedMQConsumer;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Start re-consume messages");

            if (bufferedMQConsumer.isAboutFull()) {
                LOGGER.info("Client message queue is almost full, skip popping message from local message store.");
                return;
            }

            boolean isMessageQueueFull = false;
            MessageExt[] messages = bufferedMQConsumer.getLocalMessageStore().pop(BATCH_SIZE);
            while (messages != null && messages.length > 0) {
                LOGGER.info("Popped " + messages.length + " messages from localMessageStore.");
                for (MessageExt message : messages) {
                    if (null == message) {
                        continue;
                    }

                    MessageHandler messageHandler = bufferedMQConsumer.getTopicHandlerMap().get(message.getTopic());
                    if (null == messageHandler) {
                        LOGGER.warn("No message handler for topic: " + message.getTopic());
                        bufferedMQConsumer.getLocalMessageStore().stash(message);
                        continue;
                    }

                    if (!bufferedMQConsumer.isAboutFull()) {
                        bufferedMQConsumer.getMessageQueue().put(message);
                    } else {
                        isMessageQueueFull = true;
                        bufferedMQConsumer.getLocalMessageStore().stash(message);
                    }
                }

                if (!isMessageQueueFull) {
                    messages = bufferedMQConsumer.getLocalMessageStore().pop(BATCH_SIZE);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("PopMessageService error", e);
        }

        LOGGER.info("Re-consume completes.");
    }
}

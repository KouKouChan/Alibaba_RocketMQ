package com.alibaba.rocketmq.client.producer.buffered;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;

@Deprecated
public class SendMessageCallback implements SendCallback {

    private static final Logger LOGGER = ClientLogger.getLog();

    private SendCallback hook;

    private Message message;

    private BufferedMQProducer bufferedMQProducer;

    public SendMessageCallback(BufferedMQProducer bufferedMQProducer, SendCallback sendCallback, Message message) {
        this.hook = sendCallback;
        this.message = message;
        this.bufferedMQProducer = bufferedMQProducer;
    }

    @Override
    public void onSuccess(SendResult sendResult) {
        //Update statistical data.
        bufferedMQProducer.getSuccessSendingCounter().incrementAndGet();

        //Execute user callback.
        if (null != hook) {
            try {
                hook.onSuccess(sendResult);
            } catch (Exception e) {
                LOGGER.error("Error while invoke user callback", e);
            }
        }
    }

    @Override
    public void onException(Throwable e) {

        // Update statistical data
        bufferedMQProducer.getErrorSendingCounter().incrementAndGet();

        //Stash the message and log the exception.
        bufferedMQProducer.getLocalMessageStore().stash(message);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Message stashed due to sending failure");
        }

        if (null != hook) {
            try {
                hook.onException(e);
            } catch (Exception ex) {
                LOGGER.error("Error while invoke user callback", ex);
            }

        }
    }
}

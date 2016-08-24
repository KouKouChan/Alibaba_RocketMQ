package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class FrontController implements MessageListenerConcurrently {

    private static final Logger LOGGER = ClientLogger.getLog();

    private final BufferedMQConsumer bufferedMQConsumer;


    public FrontController(BufferedMQConsumer bufferedMQConsumer) {
        this.bufferedMQConsumer = bufferedMQConsumer;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                    ConsumeConcurrentlyContext context) {
        if (null == messages) {
            LOGGER.error("Found null while preparing to consume messages in batch.");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        for (MessageExt message : messages) {
            try {
                if (null == message) {
                    continue;
                }

                String topic = message.getTopic();
                if (null == topic || !bufferedMQConsumer.getTopicHandlerMap().containsKey(topic)) {
                    LOGGER.warn("No handler provided. Message skipped.");
                    continue;
                }

                long start = System.currentTimeMillis();
                bufferedMQConsumer.getTopicHandlerMap().get(message.getTopic()).handle(message);
                long cost = System.currentTimeMillis() - start;
                bufferedMQConsumer.getStatistics().addValue(cost);
                bufferedMQConsumer.getSuccessCounter().incrementAndGet();
            } catch (Exception e) {
                LOGGER.error("Exception while handling message", e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}


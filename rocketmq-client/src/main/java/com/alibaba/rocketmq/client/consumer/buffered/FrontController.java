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

    private static final String DELAY_LEVEL = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    private int delayLevel[];


    public FrontController(BufferedMQConsumer bufferedMQConsumer) {
        this.bufferedMQConsumer = bufferedMQConsumer;
        parseDelayLevel();
    }

    private void parseDelayLevel() {
        String[] segments = DELAY_LEVEL.split("\\s");
        delayLevel = new int[segments.length];
        int index = 0;
        for (String segment : segments) {
            int num = 0;
            for (int i = 0; i < segment.length(); i++) {
                char c = segment.charAt(i);
                if (Character.isDigit(c)) {
                    num = num * 10 + (c - '0');
                } else {
                    break;
                }
            }
            if (segment.trim().endsWith("s")) {
                delayLevel[index++] = num * 1000;
            } else if (segment.trim().endsWith("m")) {
                delayLevel[index++] = num * 60 * 1000;
            } else if(segment.trim().endsWith("h")) {
                delayLevel[index++] = num * 60 * 60 * 1000;
            } else {
                throw new RuntimeException("Unsupported delay level format");
            }
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                    ConsumeConcurrentlyContext context) {
        if (null == messages) {
            LOGGER.error("Found null while preparing to consume messages in batch.");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        int ackIndex = -1;

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
                int value = bufferedMQConsumer.getTopicHandlerMap().get(message.getTopic()).handle(message);
                long cost = System.currentTimeMillis() - start;
                bufferedMQConsumer.getStatistics().addValue(cost);
                bufferedMQConsumer.getSuccessCounter().incrementAndGet();

                if (value != 0) {

                    if (value < 0) {
                        LOGGER.error("Invalid delay time in milliseconds");
                    }

                    int delayLevelWhenNextConsume = 0;
                    for (; delayLevelWhenNextConsume < delayLevel.length; delayLevelWhenNextConsume++) {
                        if (delayLevel[delayLevelWhenNextConsume] >= value) {
                            break;
                        }
                    }

                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);

                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                ackIndex++;
                context.setAckIndex(ackIndex);
            } catch (Exception e) {
                LOGGER.error("Exception while handling message", e);
                context.setDelayLevelWhenNextConsume(message.getReconsumeTimes());
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}


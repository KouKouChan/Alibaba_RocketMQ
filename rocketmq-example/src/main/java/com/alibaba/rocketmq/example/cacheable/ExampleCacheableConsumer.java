package com.alibaba.rocketmq.example.cacheable;

import com.alibaba.rocketmq.client.consumer.cacheable.CacheableConsumer;
import com.alibaba.rocketmq.client.consumer.cacheable.MessageHandler;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Random;

public class ExampleCacheableConsumer {

    private static final Random rand = new Random(System.currentTimeMillis());

    static class ExampleMessageHandler extends MessageHandler {

        /**
         * User define processing logic, implemented by ultimate business developer.
         *
         * @param message Message to process.
         * @return 0 if business logic has already properly consumed this message; positive int N if this message is
         * supposed to be consumed again N milliseconds later.
         */
        @Override
        public int handle(MessageExt message) {
            System.out.println("MessageId:" + message.getMsgId() +  message.getTopic());
            return rand.nextInt(1000) > 500 ? 1000 : 0;
        }
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {
        CacheableConsumer cacheableConsumer = new CacheableConsumer("CG_ExampleCacheableConsumer_missing_message");

        MessageHandler exampleMessageHandler = new ExampleMessageHandler();

        /**
         * Topic is strictly required.
         */
        exampleMessageHandler.setTopic("yeah_tool_topic_tracking_click_lei");

        exampleMessageHandler.setTag("*");

        cacheableConsumer.registerMessageHandler(exampleMessageHandler);

        cacheableConsumer.setCorePoolSizeForDelayTasks(1); // default 2.
        cacheableConsumer.setCorePoolSizeForWorkTasks(5); // default 10.

        cacheableConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        cacheableConsumer.setMessageModel(MessageModel.BROADCASTING);

        cacheableConsumer.start();

        System.out.println("User client starts.");
    }

}
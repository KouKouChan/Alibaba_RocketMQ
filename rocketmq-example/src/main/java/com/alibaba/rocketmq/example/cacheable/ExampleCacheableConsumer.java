package com.alibaba.rocketmq.example.cacheable;

import com.alibaba.rocketmq.client.consumer.buffered.BufferedMQConsumer;
import com.alibaba.rocketmq.client.consumer.buffered.MessageHandler;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Random;

public class ExampleCacheableConsumer {

    static class ExampleMessageHandler extends MessageHandler {
        private Random random = new Random();
        public ExampleMessageHandler() {
        }

        /**
         * User define processing logic, implemented by ultimate business developer.
         *
         * @param message Message to process.
         * @return 0 if business logic has already properly consumed this message; positive int N if this message is
         * supposed to be consumed again N milliseconds later.
         */
        @Override
        public int handle(MessageExt message) {
            System.out.println(message);
            return 0;
        }
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {
        BufferedMQConsumer bufferedMQConsumer = new BufferedMQConsumer("CG_QuickStart");

        MessageHandler exampleMessageHandler = new ExampleMessageHandler();

        /**
         * Topic is strictly required.
         */
        exampleMessageHandler.setTopic("T_QuickStart");

        exampleMessageHandler.setTag("*");

        bufferedMQConsumer.registerMessageHandler(exampleMessageHandler);

        bufferedMQConsumer.setCorePoolSizeForWorkTasks(5); // default 10.
        bufferedMQConsumer.setMaximumPoolSizeForWorkTasks(20); //default 50

        bufferedMQConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        bufferedMQConsumer.setMessageModel(MessageModel.CLUSTERING);

        bufferedMQConsumer.setMaximumNumberOfMessageBuffered(2000);

        bufferedMQConsumer.start();

        System.out.println("User client starts.");
    }

}

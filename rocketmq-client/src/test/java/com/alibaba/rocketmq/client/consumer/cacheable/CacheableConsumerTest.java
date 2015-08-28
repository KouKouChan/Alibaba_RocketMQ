package com.alibaba.rocketmq.client.consumer.cacheable;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class CacheableConsumerTest {

    private static  CacheableConsumer cacheableConsumer;

    @BeforeClass
    public static void init() throws IOException {
        cacheableConsumer = new CacheableConsumer("CG_QuickStart");
    }

    @Test
    public void testCacheable() throws Exception {

        cacheableConsumer.setMessageModel(MessageModel.CLUSTERING);
        cacheableConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageHandler messageHandler = new MessageHandler() {
            @Override
            public int handle(MessageExt message) {
                System.out.println("Done");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        };

        messageHandler.setTopic("T_QuickStart");
        cacheableConsumer.registerMessageHandler(messageHandler);
        cacheableConsumer.start();
        Thread.sleep(1000*60*50);
    }

}
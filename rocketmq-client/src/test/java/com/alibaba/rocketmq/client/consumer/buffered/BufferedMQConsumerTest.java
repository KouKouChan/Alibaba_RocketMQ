package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedMQConsumerTest {

    private static BufferedMQConsumer bufferedMQConsumer;

    @BeforeClass
    public static void init() throws IOException {
        bufferedMQConsumer = new BufferedMQConsumer("CG_QuickStart");
    }

    @Test
    public void testCacheable() throws Exception {

        bufferedMQConsumer.setMessageModel(MessageModel.CLUSTERING);
        bufferedMQConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        final AtomicInteger count = new AtomicInteger(0);

        MessageHandler messageHandler = new MessageHandler() {
            @Override
            public int handle(MessageExt message) {
                if (count.incrementAndGet() % 100 == 0) {
                    System.out.println("Consumed: " + count.intValue());
                }
                return 0;
            }
        };

        messageHandler.setTopic("T_QuickStart");
        bufferedMQConsumer.registerMessageHandler(messageHandler);
        bufferedMQConsumer.start();
        Thread.sleep(1000*60*50);



    }

}
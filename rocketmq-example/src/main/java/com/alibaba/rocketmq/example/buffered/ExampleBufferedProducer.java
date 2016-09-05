package com.alibaba.rocketmq.example.buffered;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.buffered.BufferedMQProducer;
import com.alibaba.rocketmq.client.producer.selector.Region;
import com.alibaba.rocketmq.common.message.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ExampleBufferedProducer {

    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0L);

    private static byte[] messageBody = new byte[1024];

    static {
        Arrays.fill(messageBody, (byte) 'x');
    }

    public static void main(String[] args) throws MQClientException, IOException {
        BufferedMQProducer bufferedMQProducer = new BufferedMQProducer("PG_QuickStart");
        bufferedMQProducer.setSendMsgTimeout(30000);
        bufferedMQProducer.registerCallback(new ExampleSendCallback());
        bufferedMQProducer.setTargetRegion(Region.US_WEST);
        bufferedMQProducer.start();

        for (int i = 0; i < 1000; i++) {
            bufferedMQProducer.send(buildMessages(100));
        }
    }

    public static Message[] buildMessages(int n) {
        Message[] messages = new Message[n];
        for (int i = 0; i < n; i++) {
            messages[i] = new Message("T_QuickStart", messageBody);
            messages[i].putUserProperty("sequenceId", String.valueOf(SEQUENCE_GENERATOR.incrementAndGet()));
        }
        return messages;
    }

}

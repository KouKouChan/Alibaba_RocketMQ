package com.alibaba.rocketmq.example.buffered;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.buffered.BufferedMQProducer;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        bufferedMQProducer.start();

        bufferedMQProducer.send(buildMessages(100));
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

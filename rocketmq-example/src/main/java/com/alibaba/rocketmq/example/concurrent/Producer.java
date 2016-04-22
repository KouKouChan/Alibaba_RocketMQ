package com.alibaba.rocketmq.example.concurrent;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.concurrent.MultiThreadMQProducer;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {

    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0L);

    private static final Random RANDOM = new Random();

    private static final Logger LOGGER = ClientLogger.getLog();

    private static byte[] messageBody = new byte[1024];

    static {
        Arrays.fill(messageBody, (byte) 'x');
    }

    public static void main(String[] args) {
        int count = 0;
        if (args.length > 0) {
            count = Integer.parseInt(args[0]);
        } else {
            count = -1;
        }
        final AtomicLong successCount = new AtomicLong(0L);
        final AtomicLong lastSent = new AtomicLong(0L);
        final MultiThreadMQProducer producer = MultiThreadMQProducer.configure()
                .configureProducerGroup("PG_QuickStart")
                .configureRetryTimesBeforeSendingFailureClaimed(3)
                .configureSendMessageTimeOutInMilliSeconds(3000)
                .configureDefaultTopicQueueNumber(16)
                .build();
        producer.registerCallback(new ExampleSendCallback(successCount));

        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TPSService"))
                .scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        long currentSuccessSent = successCount.longValue();
                        LOGGER.info("TPS: " + (currentSuccessSent - lastSent.longValue()) +
                                ". Semaphore available number:" + producer.getSemaphore().availablePermits());
                        lastSent.set(currentSuccessSent);
                    }
                }, 0, 1, TimeUnit.SECONDS);

        if (count < 0) {
            final AtomicLong adder = new AtomicLong(0L);
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MessageManufactureService"))
                    .scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Message[] messages = buildMessages(RANDOM.nextInt(800));
                                producer.send(messages);
                                adder.incrementAndGet();
                                if (adder.longValue() % 1000 == 0) {
                                    LOGGER.info(messages.length + " messages from client are required to send.");
                                }
                            } catch (Exception e) {
                                LOGGER.error("Message manufacture caught an exception.", e);
                            }
                        }
                    }, 3000, 100, TimeUnit.MILLISECONDS);
        } else {
            long start = System.currentTimeMillis();
            Message[] messages = buildMessages(count);
            producer.send(messages);
            LOGGER.info("Messages are sent in async manner. Cost " + (System.currentTimeMillis() - start) + "ms");
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

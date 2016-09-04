package com.alibaba.rocketmq.example.smoking;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {

    private static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption("n", "namesrv", true, "Name server");
        options.addOption("g", "group", true, "Producer Group");
        options.addOption("t", "topic", true, "Topic");
        options.addOption("c", "count", true, "Count");

        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Send Message", options);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String producerGroup = commandLine.getOptionValue("g", "Smoking");
        String topic = commandLine.getOptionValue("t", "TestTopic");
        long number = Long.parseLong(commandLine.getOptionValue("c", "10000"));
        String namesrv = commandLine.getOptionValue("n");

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        if (null != namesrv) {
            producer.setNamesrvAddr(namesrv);
        }

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        try {
            producer.start();
            byte[] body = new byte[1024];
            Arrays.fill(body, (byte)'x');
            Message message = new Message(topic, body);
            long start = System.currentTimeMillis();
            CountDownLatch countDownLatch = new CountDownLatch((int)number);
            final AtomicInteger successCount = new AtomicInteger();
            final AtomicInteger prevSuccessCount = new AtomicInteger();
            final AtomicInteger errorCount = new AtomicInteger();
            final AtomicInteger prevErrorCount = new AtomicInteger();

            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    int success = successCount.get();
                    int error = errorCount.get();
                    int successDiff = success - prevSuccessCount.get();
                    int errorDiff = error - prevErrorCount.get();

                    prevSuccessCount.set(success);
                    prevErrorCount.set(error);

                    System.out.println("Success QPS: " + (successDiff / 10));
                    System.out.println("Error QPS: " + (errorDiff / 10));
                }
            }, 10, 10, TimeUnit.SECONDS);

            for (int i = 0; i < number; i++) {
                try {
                    producer.send(message, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            successCount.incrementAndGet();
                        }

                        @Override
                        public void onException(Throwable e) {
                            errorCount.incrementAndGet();
                        }
                    });
                } catch (Exception e) {
                    LOGGER.error("Send Failed", e);
                }
            }

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                LOGGER.error("Thread Interrupted", e);
            }
            long interval = System.currentTimeMillis() - start;
            executorService.shutdown();
            System.out.println("Success QPS: " + (successCount.get() * 1000L / interval));
            System.out.println("Error QPS: " + (errorCount.get() * 1000L / interval));
        } catch (MQClientException e) {
            LOGGER.error("Start producer failed", e);
        } finally {
            producer.shutdown();
        }
    }

}

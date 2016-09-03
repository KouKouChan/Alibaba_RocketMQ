package com.alibaba.rocketmq.example.smoking;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.commons.cli.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer {

    public static void main(String[] args) throws ParseException, MQClientException {

        Options options = new Options();
        options.addOption("g", "group", true, "Consumer Group");
        options.addOption("t", "topic", true, "Topic");
        options.addOption("n", "namesrv", true, "Name Server");
        options.addOption("b", "batch", true, "Pull batch size");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String consumerGroup = commandLine.getOptionValue("g", "Smoking");
        String topic = commandLine.getOptionValue("t", "TestTopic");

        if (!commandLine.hasOption("n")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("Consumer", options);
            System.exit(1);
        }

        String namesrv = commandLine.getOptionValue("n");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.subscribe(topic, null);
        consumer.setNamesrvAddr(namesrv);
        consumer.setPullBatchSize(Integer.parseInt(commandLine.getOptionValue("b", "32")));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        final AtomicInteger prev = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                count.addAndGet(msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        consumer.start();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int curr = count.get();
                int diff = curr - prev.get();
                prev.set(curr);
                System.out.println("QPS: " + (diff / 10));
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
}

package com.alibaba.rocketmq.example.smoking;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.commons.cli.*;

import java.util.List;

public class Consumer {

    public static void main(String[] args) throws ParseException, MQClientException {

        Options options = new Options();
        options.addOption("g", "group", true, "Consumer Group");
        options.addOption("t", "topic", true, "Topic");
        options.addOption("n", "namesrv", true, "Name Server");

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

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}

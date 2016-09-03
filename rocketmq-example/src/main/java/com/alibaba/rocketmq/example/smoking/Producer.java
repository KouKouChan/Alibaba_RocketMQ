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

        try {
            producer.start();
            byte[] body = new byte[1024];
            Arrays.fill(body, (byte)'x');
            Message message = new Message(topic, body);
            for (int i = 0; i < number; i++) {
                try {
                    SendResult sendResult = producer.send(message);
                    LOGGER.debug(sendResult.getMsgId());
                } catch (Exception e) {
                    LOGGER.error("Send Failed", e);
                }
            }
        } catch (MQClientException e) {
            LOGGER.error("Start producer failed", e);
        } finally {
            producer.shutdown();
        }
    }

}

package com.ndpmedia.rocketmq.babel;

import com.alibaba.rocketmq.client.consumer.buffered.MessageHandler;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerService implements Consumer.Iface {

    private static final Logger LOGGER = ClientLogger.getLog();

    private static final String CLASS_NAME = ConsumerService.class.getName();

    private static final int DEFAULT_MESSAGE_BATCH_SIZE = 128;
    private int messageBatchSize = DEFAULT_MESSAGE_BATCH_SIZE;
    private CustomBufferedMQConsumer consumer;
    private LinkedBlockingQueue<MessageExt> messageQueue = new LinkedBlockingQueue<MessageExt>(1024);

    public ConsumerService() throws MQClientException, InterruptedException {
        Properties properties = Helper.getConfig();
        String consumerGroup = properties.getProperty("consumer_group");
        String topicInfo = properties.getProperty("topic_info");
        consumer = new CustomBufferedMQConsumer(consumerGroup);

        if("cluster".equals(properties.getProperty("message_model"))) {
            consumer.setMessageModel(MessageModel.CLUSTERING);
        } else {
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }

        String[] topicList = topicInfo.split(";");
        for (String topicItem : topicList) {
            String[] topicAndTag = topicItem.split(",");
            if (topicAndTag.length != 2) {
                throw new RuntimeException("Configuration file format illegal. Please refer to sample_rocketmq_client_setting.properties file");
            }

            MessageHandler messageHandler = new ThriftMessageHandler(this);
            messageHandler.setTopic(topicAndTag[0]);
            messageHandler.setTag(topicAndTag[1]);
            consumer.registerMessageHandler(messageHandler);
        }

        consumer.start();
    }

    private com.ndpmedia.rocketmq.babel.MessageExt wrap(MessageExt msg) {
        com.ndpmedia.rocketmq.babel.MessageExt message = new com.ndpmedia.rocketmq.babel.MessageExt();
        message.setTopic(msg.getTopic());
        message.setSysFlag(msg.getSysFlag());
        message.setFlag(msg.getFlag());
        message.setProperties(msg.getProperties());
        message.setQueueId(msg.getQueueId());
        message.setQueueOffset(msg.getQueueOffset());
        message.setStoreSize(msg.getStoreSize());
        message.setStoreTimestamp(msg.getStoreTimestamp());
        message.setStoreHost(null == msg.getBornHost() ? null : msg.getBornHost().toString());
        message.setBornTimestamp(msg.getBornTimestamp());
        message.setBornHost(null == msg.getBornHost() ? null : msg.getBornHost().toString());
        message.setBodyCRC(msg.getBodyCRC());
        message.setData(msg.getBody());
        message.setReconsumeTimes(msg.getReconsumeTimes());
        message.setCommitLogOffset(msg.getCommitLogOffset());
        message.setMsgId(msg.getMsgId());
        return message;
    }

    public LinkedBlockingQueue<MessageExt> getMessageQueue() {
        return messageQueue;
    }

    @Override
    public List<com.ndpmedia.rocketmq.babel.MessageExt> pull() throws TException {
        List<com.ndpmedia.rocketmq.babel.MessageExt> messageList =
                new ArrayList<com.ndpmedia.rocketmq.babel.MessageExt>(messageBatchSize);
        MessageExt msg = messageQueue.poll();

        int count = 0;
        while (null != msg && count++ < messageBatchSize) {
            messageList.add(wrap(msg));
            msg = messageQueue.poll();
        }

        return messageList;
    }

    @Override
    public void stop() throws TException {
        final String signature = CLASS_NAME + "#stop()";
        LOGGER.debug("Enter " + signature);
        try {
            consumer.stopReceiving();

            MessageExt msg = messageQueue.poll();
            while (null != msg) {
                msg = messageQueue.poll();
            }
            consumer.shutdown();
        } catch (InterruptedException e) {
            LOGGER.error("Failed to stop", e);
            throw new TException("Failed to stop", e);
        }
        LOGGER.debug("Exit " + signature);
    }
}


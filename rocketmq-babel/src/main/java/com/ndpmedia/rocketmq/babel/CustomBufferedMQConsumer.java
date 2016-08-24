package com.ndpmedia.rocketmq.babel;

import com.alibaba.rocketmq.client.consumer.buffered.BufferedMQConsumer;

public class CustomBufferedMQConsumer extends BufferedMQConsumer {


    public CustomBufferedMQConsumer(String consumerGroupName) {
        super(consumerGroupName);
    }

    public void stopReceiving() throws InterruptedException {
    }
}



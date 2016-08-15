package com.ndpmedia.rocketmq.babel;

import com.alibaba.rocketmq.client.consumer.buffered.MessageHandler;

class ThriftMessageHandler extends MessageHandler {

    private ConsumerService consumerService;

    public ThriftMessageHandler(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @Override
    public int handle(com.alibaba.rocketmq.common.message.MessageExt message) {
        try {
            consumerService.getMessageQueue().put(message);
        } catch (InterruptedException e) {
            return 500;
        }
        return 0;
    }
}

package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.net.InetSocketAddress;

public final class StoreHelper {

    private StoreHelper() {
    }

    public static MessageExt wrap(Message message) {
        if (message instanceof MessageExt) {
            return (MessageExt)message;
        }

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(message.getTopic());
        messageExt.setFlag(message.getFlag());
        messageExt.setBody(message.getBody());
        MessageAccessor.setProperties(messageExt, message.getProperties());

        messageExt.setBornHost(new InetSocketAddress(0));
        messageExt.setStoreHost(new InetSocketAddress(0));

        return messageExt;
    }
}

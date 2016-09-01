package com.alibaba.rocketmq.example.smoking;

import com.alibaba.rocketmq.client.store.DefaultLocalMessageStore;
import com.alibaba.rocketmq.client.store.LocalMessageStore;
import com.alibaba.rocketmq.common.message.Message;

import java.io.IOException;
import java.util.Arrays;

public class LocalLoad {
    public static void main(String[] args) throws IOException {
        final LocalMessageStore localMessageStore = new DefaultLocalMessageStore("Test");
        localMessageStore.start();

        final byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        final Message message = new Message("TestTopic", data);
        Message[] messages;
        while (null != (messages = localMessageStore.pop(100))) {
            for (Message msg : messages) {
                System.out.println("Topic: " + message.getTopic().equals(msg.getTopic()));
                System.out.println("Body: " + Arrays.equals(message.getBody(), msg.getBody()));
            }
        }
        localMessageStore.close();
    }
}

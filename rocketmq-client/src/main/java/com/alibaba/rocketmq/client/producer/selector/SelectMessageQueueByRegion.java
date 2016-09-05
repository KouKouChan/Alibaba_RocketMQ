package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SelectMessageQueueByRegion implements MessageQueueSelector {

    private Region target;

    private ConcurrentHashMap<MessageQueue, Integer> messageQueueRegionMap = new ConcurrentHashMap<>();

    public SelectMessageQueueByRegion(Region target) {
        if (null == target) {
            this.target = Region.SAME;
        } else {
            this.target = target;
        }
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {

        if (Region.SAME == target) {
            target = Region.parse(Integer.parseInt(Util.LOCAL_DATA_CENTER_ID));
        }

        if (Region.ANY == target) {
            return Util.random(mqs);
        }

        List<MessageQueue> list = new ArrayList<>();

        for (MessageQueue messageQueue : mqs) {
            if (!messageQueueRegionMap.containsKey(messageQueue)) {
                messageQueueRegionMap.put(messageQueue, Util.getRegionIndex(messageQueue));
            }

            if (messageQueueRegionMap.get(messageQueue) == target.getIndex()) {
                list.add(messageQueue);
            }
        }

        if (list.isEmpty()) {
            return Util.random(mqs);
        }

        return Util.random(list);
    }
}

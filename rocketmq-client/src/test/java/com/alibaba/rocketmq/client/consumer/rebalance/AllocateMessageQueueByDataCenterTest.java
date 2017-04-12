package com.alibaba.rocketmq.client.consumer.rebalance;

import com.alibaba.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateMessageQueueByDataCenterTest {

    @Test
    public void testAllocateByCircle() throws Exception {
        List<MessageQueue> messageQueues = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageQueue messageQueue = new MessageQueue("Test", "DefaultCluster_2_broker" + (i + 1), i);
            messageQueues.add(messageQueue);
        }

        List<String> clientIds = new ArrayList<>();
        clientIds.add("C1");
        clientIds.add("C2");
        clientIds.add("C3");

        Map<String, List<MessageQueue>> result = new HashMap<>();

        Class clazz = AllocateMessageQueueByDataCenter.class;
        Method method = clazz.getDeclaredMethod("allocateByCircle", new Class[] {List.class, List.class, HashMap.class});
        method.setAccessible(true);
        method.invoke(null, messageQueues, clientIds, result);
        for (Map.Entry<String, List<MessageQueue>> next : result.entrySet()) {
            System.out.println(next.getKey() + "-->");
            for (MessageQueue messageQueue : next.getValue()) {
                System.out.println(messageQueue);
            }
        }
    }
}
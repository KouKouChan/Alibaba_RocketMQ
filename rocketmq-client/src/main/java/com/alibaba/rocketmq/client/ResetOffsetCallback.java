package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.Map;

public interface ResetOffsetCallback {
    void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable);
}

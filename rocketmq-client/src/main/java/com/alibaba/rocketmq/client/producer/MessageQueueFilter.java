package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * MessageQueueFilter
 * User: penuel (penuel.leo@gmail.com)
 * Date: 15/8/11 上午11:58
 * Desc:
 */

public interface MessageQueueFilter {
    public List<MessageQueue> filter(final List<MessageQueue> mqs, final Object arg);
}

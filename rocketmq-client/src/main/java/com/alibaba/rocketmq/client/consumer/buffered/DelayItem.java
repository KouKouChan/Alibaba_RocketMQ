package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayItem implements Delayed {

    private final MessageExt message;

    private final long delay;

    public DelayItem(MessageExt message, long delay) {
        this.message = message;
        this.delay = delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        if (unit == TimeUnit.MILLISECONDS) {
            return delay;
        } else {
            throw new RuntimeException("Unsupported Time Unit");
        }
    }

    public MessageExt getMessage() {
        return message;
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }
}

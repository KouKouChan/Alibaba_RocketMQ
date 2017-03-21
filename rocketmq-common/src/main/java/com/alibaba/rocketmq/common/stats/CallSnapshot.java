package com.alibaba.rocketmq.common.stats;

public class CallSnapshot {
    private final long timestamp;
    private final long times;

    private final long value;


    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }


    public long getTimestamp() {
        return timestamp;
    }


    public long getTimes() {
        return times;
    }


    public long getValue() {
        return value;
    }
}

package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.common.SystemClock;
import com.alibaba.rocketmq.remoting.statistics.LatencyStatisticsItem;

public class RpcContext {
    private final long createTimePoint;
    private final LatencyStatisticsItem latencyStatisticsItem;
    private final SystemClock systemClock;

    public RpcContext(final long createTimePoint,
        final SystemClock systemClock,
        final LatencyStatisticsItem latencyStatisticsItem) {
        this.createTimePoint = createTimePoint;
        this.systemClock = systemClock;
        this.latencyStatisticsItem = latencyStatisticsItem;
    }

    public long getCreateTimePoint() {
        return createTimePoint;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public LatencyStatisticsItem getLatencyStatisticsItem() {
        return latencyStatisticsItem;
    }
}

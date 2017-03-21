package com.alibaba.rocketmq.remoting.statistics;

import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class LatencyStatisticsItemTest {

    @Test
    public void testReport() {
        LatencyStatisticsItem latencyStatisticsItem = new LatencyStatisticsItem("Test");
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            latencyStatisticsItem.add(random.nextInt(1100));
        }
        String report = latencyStatisticsItem.report(latencyStatisticsItem.rotate());
        Assert.assertNotNull(report);
    }

}
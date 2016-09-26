package com.alibaba.rocketmq.remoting.common;

import org.junit.Assert;
import org.junit.Test;

public class RemotingHelperTest {

    @Test
    public void filterIP() throws Exception {
        String ipCSV = "127.0.0.1,127.0.1.1,8.8.4.4,10.1.36.10,8.8.8.8:10911";
        Assert.assertEquals("10.1.36.10", RemotingHelper.filterIP(ipCSV));
    }

}
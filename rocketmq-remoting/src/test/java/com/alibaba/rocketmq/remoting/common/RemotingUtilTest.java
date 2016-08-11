package com.alibaba.rocketmq.remoting.common;

import org.junit.Assert;
import org.junit.Test;

public class RemotingUtilTest {

    @Test
    public void testNetwork() {
        System.out.println(RemotingUtil.getLocalAddress(false));
        System.out.println(RemotingUtil.getLocalAddress(false).split("\\.")[1]);
    }

    @Test
    public void testIsPrivateIPv4Address() throws Exception {
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("127.1.1.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("127.0.0.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("192.168.2.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("172.31.2.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("172.16.2.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("172.19.2.1"));
        Assert.assertTrue(RemotingUtil.isPrivateIPv4Address("172.24.2.1"));
        Assert.assertFalse(RemotingUtil.isPrivateIPv4Address("172.15.2.1"));
        Assert.assertFalse(RemotingUtil.isPrivateIPv4Address("172.1.2.1"));
        Assert.assertFalse(RemotingUtil.isPrivateIPv4Address("8.8.8.8"));
        Assert.assertFalse(RemotingUtil.isPrivateIPv4Address("121.8.8.8"));

    }
}
package com.alibaba.rocketmq.remoting.netty;

public class NettySystemConfig {
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable = "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static boolean NettyPooledByteBufAllocatorEnable = Boolean.parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "true"));

    public static final String SystemPropertySocketSndbufSize = "com.rocketmq.remoting.socket.sndbuf.size";
    public static int SocketSndbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "8388608"));

    public static final String SystemPropertySocketRcvbufSize = "com.rocketmq.remoting.socket.rcvbuf.size";
    public static int SocketRcvbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "8388608"));

    public static final String SystemPropertyClientAsyncSemaphoreValue = //
            "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static int ClientAsyncSemaphoreValue = //
            Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "2048"));

    public static final String SystemPropertyClientOnewaySemaphoreValue = //
            "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    public static int ClientOnewaySemaphoreValue = //
            Integer.parseInt(System.getProperty(SystemPropertyClientOnewaySemaphoreValue, "2048"));

    public static final String SYSTEM_NETTY_CONNECT_TIMEOUT = "com.rocketmq.remoting.connect.timeout";
    public static int NETTY_CONNECT_TIMEOUT = Integer.parseInt(System.getProperty(SYSTEM_NETTY_CONNECT_TIMEOUT, "30000"));

    public static final String SYSTEM_NETTY_IO_TIMEOUT = "com.rocketmq.remoting.io.timeout";
    public static int NETTY_IO_TIMEOUT = Integer.parseInt(System.getProperty(SYSTEM_NETTY_IO_TIMEOUT, "30000"));

    public static final String SYSTEM_NETTY_HEARTBEAT_TIMEOUT = "com.rocketmq.remoting.heartbeat.timeout";
    public static int NETTY_HEARTBEAT_TIMEOUT = Integer.parseInt(System.getProperty(SYSTEM_NETTY_HEARTBEAT_TIMEOUT, "15000"));
}

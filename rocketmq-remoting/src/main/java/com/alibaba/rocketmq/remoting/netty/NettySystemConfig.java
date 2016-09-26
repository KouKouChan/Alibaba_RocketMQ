/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.remoting.netty;

public class NettySystemConfig {
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable =
            "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static final String SystemPropertySocketSndbufSize = //
            "com.rocketmq.remoting.socket.sndbuf.size";
    public static final String SystemPropertySocketRcvbufSize = //
            "com.rocketmq.remoting.socket.rcvbuf.size";
    public static final String SystemPropertyClientAsyncSemaphoreValue = //
            "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static final String SystemPropertyClientOnewaySemaphoreValue = //
            "com.rocketmq.remoting.clientOnewaySemaphoreValue";

    public static final String SystemPropertyClientSocketOverTLS = "com.rocketmq.remoting.clientSocketOverTLS";

    public static final String SystemPropertyServerSocketOverTLS = "com.rocketmq.remoting.serverSocketOverTLS";

    public static final String SystemPropertyConnectTimeout = "com.rocketmq.remoting.connect.timeout";

    public static final boolean NettyPooledByteBufAllocatorEnable = //
            Boolean.parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "true"));

    /**
     * Default send buffer size: 8M.
     */
    public static final int socketSndbufSize = //
            Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "8388608"));

    /**
     * Default receive buffer size: 8M.
     */
    public static final int socketRcvbufSize = //
            Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "8388608"));

    public static final int ClientAsyncSemaphoreValue = //
            Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "65535"));
    public static final int ClientOnewaySemaphoreValue = //
            Integer.parseInt(System.getProperty(SystemPropertyClientOnewaySemaphoreValue, "65535"));

    public static final boolean CLIENT_SOCKET_OVER_TLS = Boolean.parseBoolean(System.getProperty(SystemPropertyClientSocketOverTLS, "true"));

    public static final boolean SERVER_SOCKET_OVER_TLS = Boolean.parseBoolean(System.getProperty(SystemPropertyServerSocketOverTLS, "true"));

    public static final int CONNECT_TIMEOUT = Integer.parseInt(System.getProperty(SystemPropertyConnectTimeout, "30000"));

}

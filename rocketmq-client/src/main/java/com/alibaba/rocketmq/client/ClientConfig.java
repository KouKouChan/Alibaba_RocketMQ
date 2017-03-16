/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Producer与Consumer的公共配置
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class ClientConfig {
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int pollNameServerInterval = 1000 * 30;
    private int heartbeatBrokerInterval = 1000 * 30;
    private int heartbeatTimeout = NettySystemConfig.NETTY_HEARTBEAT_TIMEOUT;
    private int networkTimeout = NettySystemConfig.NETTY_IO_TIMEOUT;
    private int persistConsumerOffsetInterval = 1000 * 5;
    private ClientType clientType;
    private WeakReference weakReference;
    private Integer clientAsyncSemaphoreValue;
    private Integer clientOnewaySemaphoreValue;

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());

        return sb.toString();
    }


    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }


    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.heartbeatTimeout = cc.heartbeatTimeout;
        this.networkTimeout = cc.networkTimeout;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.clientType = cc.clientType;
        this.weakReference = cc.weakReference;
        this.clientAsyncSemaphoreValue = cc.clientAsyncSemaphoreValue;
        this.clientOnewaySemaphoreValue = cc.clientOnewaySemaphoreValue;
    }


    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.heartbeatTimeout = heartbeatTimeout;
        cc.networkTimeout = networkTimeout;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.clientType = clientType;
        cc.weakReference = weakReference;
        cc.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
        cc.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
        return cc;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public String getClientIP() {
        return clientIP;
    }


    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }


    public String getInstanceName() {
        return instanceName;
    }


    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }


    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }


    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }


    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }


    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }


    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }


    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public void setClientType(ClientType clientType) {
        this.clientType = clientType;
    }

    public boolean isProducer() {
        return clientType == ClientType.PRODUCER;
    }

    public boolean isConsumer() {
        return clientType == ClientType.CONSUMER;
    }

    public boolean isAdminTool() {
        return clientType == ClientType.ADMIN_TOOL;
    }

    public WeakReference getWeakReference() {
        return weakReference;
    }

    public void setWeakReference(WeakReference weakReference) {
        this.weakReference = weakReference;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(int heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getNetworkTimeout() {
        return networkTimeout;
    }

    public void setNetworkTimeout(int networkTimeout) {
        this.networkTimeout = networkTimeout;
    }

    public Integer getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public void setClientAsyncSemaphoreValue(Integer clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public Integer getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public void setClientOnewaySemaphoreValue(Integer clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }

    @Override
    public String toString() {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName="
                + instanceName + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads
                + ", pollNameServerInterval=" + pollNameServerInterval + ", heartbeatBrokerInterval="
                + heartbeatBrokerInterval + ", heartbeatTimeout=" + heartbeatTimeout
                + ", networkTimeout=" + networkTimeout
                + ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval
                + ", clientType=" + clientType
                + ", clientAsyncSemaphoreValue=" + clientAsyncSemaphoreValue
                + ", clientOnewaySemaphoreValue=" + clientOnewaySemaphoreValue
                + "]";
    }
}

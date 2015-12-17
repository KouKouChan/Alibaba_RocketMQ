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
package com.alibaba.rocketmq.tools.admin;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.admin.MQAdminExtInner;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.admin.RollbackStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.tools.admin.api.MessageTrack;
import com.alibaba.rocketmq.tools.admin.api.TrackType;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;


/**
 * 所有运维接口都在这里实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class DefaultMQAdminExtImpl implements MQAdminExt, MQAdminExtInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQAdminExt defaultMQAdminExt;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mqClientInstance;
    private RPCHook rpcHook;


    public DefaultMQAdminExtImpl(DefaultMQAdminExt defaultMQAdminExt) {
        this(defaultMQAdminExt, null);
    }


    public DefaultMQAdminExtImpl(DefaultMQAdminExt defaultMQAdminExt, RPCHook rpcHook) {
        this.defaultMQAdminExt = defaultMQAdminExt;
        this.rpcHook = rpcHook;
    }


    @Override
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.START_FAILED;

            this.defaultMQAdminExt.changeInstanceNameToPID();

            this.mqClientInstance =
                    MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQAdminExt,
                        rpcHook);

            boolean registerOK =
                    mqClientInstance.registerAdminExt(this.defaultMQAdminExt.getAdminExtGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The adminExt group[" + this.defaultMQAdminExt.getAdminExtGroup()
                        + "] has created already, specified another name please."//
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mqClientInstance.start();

            log.info("the adminExt [{}] start OK", this.defaultMQAdminExt.getAdminExtGroup());

            this.serviceState = ServiceState.RUNNING;
            break;
        case RUNNING:
        case START_FAILED:
        case SHUTDOWN_ALREADY:
            throw new MQClientException("The AdminExt service state not OK, maybe started once, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        default:
            break;
        }
    }


    @Override
    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.mqClientInstance.unregisterAdminExt(this.defaultMQAdminExt.getAdminExtGroup());
            this.mqClientInstance.shutdown();

            log.info("the adminExt [{}] shutdown OK", this.defaultMQAdminExt.getAdminExtGroup());
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().createTopic(addr,
            this.defaultMQAdminExt.getCreateTopicKey(), config, 3000);
    }

    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().createTopic(addr,
                this.defaultMQAdminExt.getCreateTopicKey(), config, timeoutMillis);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().createSubscriptionGroup(addr, config, 3000);
    }

    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().createSubscriptionGroup(addr, config, timeoutMillis);
    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SubscriptionGroupWrapper fetchAllSubscriptionGroups(String brokerAddress, long timeout) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        return mqClientInstance.getMQClientAPIImpl().getSubscriptionGroupsFromBroker(brokerAddress, timeout);
    }

    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public TopicStatsTable examineTopicStats(String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return examineTopicStats(topic, 3000L);
    }

    @Override
    public TopicStatsTable examineTopicStats(String topic, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {

        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        TopicStatsTable topicStatsTable = new TopicStatsTable();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                TopicStatsTable tst =
                        this.mqClientInstance.getMQClientAPIImpl().getTopicStatsInfo(addr, topic, timeoutMillis);
                topicStatsTable.getOffsetTable().putAll(tst.getOffsetTable());
            }
        }

        if (topicStatsTable.getOffsetTable().isEmpty()) {
            throw new MQClientException("Not found the topic stats info", null);
        }

        return topicStatsTable;
    }


    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        return examineConsumeStats(consumerGroup, 15000L);
    }

    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        String retryTopic = MixAll.getRetryTopic(consumerGroup);
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(retryTopic, timeoutMillis);
        ConsumeStats result = new ConsumeStats();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                // 由于查询时间戳会产生IO操作，可能会耗时较长，所以超时时间设置为15s
                ConsumeStats consumeStats =
                        this.mqClientInstance.getMQClientAPIImpl()
                                .getConsumeStats(addr, consumerGroup, timeoutMillis);
                result.getOffsetTable().putAll(consumeStats.getOffsetTable());
                long value = result.getConsumeTps() + consumeStats.getConsumeTps();
                result.setConsumeTps(value);
            }
        }

        if (result.getOffsetTable().isEmpty()) {
            throw new MQClientException(
                    "Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not consume any message",
                    null);
        }

        return result;
    }


    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        return this.mqClientInstance.getMQClientAPIImpl().getBrokerClusterInfo(3000);
    }

    @Override
    public ClusterInfo examineBrokerClusterInfo(long timeoutMillis) throws InterruptedException, MQBrokerException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        return this.mqClientInstance.getMQClientAPIImpl().getBrokerClusterInfo(timeoutMillis);
    }


    @Override
    public TopicRouteData examineTopicRouteInfo(String topic) throws RemotingException, MQClientException,
            InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
    }

    @Override
    public TopicRouteData examineTopicRouteInfo(String topic, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        // TODO Auto-generated method stub

    }


    @Override
    public String getKVConfig(String namespace, String key) throws RemotingException, MQClientException,
            InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getKVConfigValue(namespace, key, 3000);
    }

    @Override
    public String getKVConfig(String namespace, String key, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getKVConfigValue(namespace, key, timeoutMillis);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        this.mqClientInstance.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mqClientInstance.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mqClientInstance.getMQAdminImpl().maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mqClientInstance.getMQAdminImpl().minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mqClientInstance.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mqClientInstance.getMQAdminImpl().viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mqClientInstance.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return examineConsumerConnectionInfo(consumerGroup, 3000L);
    }

    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup, long timeoutMillis)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        String topic = MixAll.getRetryTopic(consumerGroup);
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        ConsumerConnection result = new ConsumerConnection();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                return this.mqClientInstance.getMQClientAPIImpl().getConsumerConnectionList(addr,
                        consumerGroup, timeoutMillis);
            }
        }

        if (result.getConnectionSet().isEmpty()) {
            throw new MQClientException("Not found the consumer group connection", null);
        }

        return result;
    }


    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return examineProducerConnectionInfo(producerGroup, topic, 3000L);
    }

    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, String topic, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {

        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        ProducerConnection result = new ProducerConnection();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                return this.mqClientInstance.getMQClientAPIImpl().getProducerConnectionList(addr,
                        producerGroup, timeoutMillis);
            }
        }

        if (result.getConnectionSet().isEmpty()) {
            throw new MQClientException("Not found the consumer group connection", null);
        }

        return result;
    }


    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        return this.mqClientInstance.getMQClientAPIImpl()
            .wipeWritePermOfBroker(namesrvAddr, brokerName, 3000);
    }

    @Override
    public int wipeWritePermOfBroker(String namesrvAddr, String brokerName, long timeoutMillis)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        return this.mqClientInstance.getMQClientAPIImpl()
                .wipeWritePermOfBroker(namesrvAddr, brokerName, timeoutMillis);
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.mqClientInstance.getMQClientAPIImpl().getNameServerAddressList();
    }


    @Override
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getTopicListFromNameServer(3000);
    }

    @Override
    public TopicList fetchAllTopicList(long timeoutMillis) throws RemotingException, MQClientException,
            InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getTopicListFromNameServer(timeoutMillis);
    }


    @Override
    public KVTable fetchBrokerRuntimeStats(final String brokerAddr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.mqClientInstance.getMQClientAPIImpl().getBrokerRuntimeInfo(brokerAddr, 3000);
    }

    @Override
    public KVTable fetchBrokerRuntimeStats(String brokerAddr, long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.mqClientInstance.getMQClientAPIImpl().getBrokerRuntimeInfo(brokerAddr, timeoutMillis);
    }


    @Override
    public void deleteTopicInBroker(Set<String> addrs, String topic) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        for (String addr : addrs) {
            this.mqClientInstance.getMQClientAPIImpl().deleteTopicInBroker(addr, topic, 3000);
        }
    }

    @Override
    public void deleteTopicInBroker(Set<String> addrs, String topic, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        for (String addr : addrs) {
            this.mqClientInstance.getMQClientAPIImpl().deleteTopicInBroker(addr, topic, timeoutMillis);
        }
    }


    @Override
    public void deleteTopicInNameServer(Set<String> nameServerAddresses,
                                        String topic,
                                        String brokerAddresses)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        deleteTopicInNameServer(nameServerAddresses, topic, brokerAddresses, 3000L);
    }

    @Override
    public void deleteTopicInNameServer(Set<String> nameServerAddresses, String topic, String brokerAddresses, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        if (nameServerAddresses == null) {
            String ns = this.mqClientInstance.getMQClientAPIImpl().fetchNameServerAddr();
            nameServerAddresses = new HashSet(Arrays.asList(ns.split(";")));
        }
        for (String nameServerAddress : nameServerAddresses) {
            this.mqClientInstance.getMQClientAPIImpl().deleteTopicInNameServer(nameServerAddress, topic, brokerAddresses, timeoutMillis);
        }
    }


    @Override
    public void deleteSubscriptionGroup(String addr, String groupName) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().deleteSubscriptionGroup(addr, groupName, 3000);
    }

    @Override
    public void deleteSubscriptionGroup(String addr, String groupName, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().deleteSubscriptionGroup(addr, groupName, timeoutMillis);
    }


    @Override
    public void createAndUpdateKvConfig(String namespace, String key, String value) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().putKVConfigValue(namespace, key, value, 3000);
    }

    @Override
    public void createAndUpdateKvConfig(String namespace, String key, String value, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().putKVConfigValue(namespace, key, value, timeoutMillis);
    }


    @Override
    public void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().deleteKVConfigValue(namespace, key, 3000);
    }

    @Override
    public void deleteKvConfig(String namespace, String key, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.mqClientInstance.getMQClientAPIImpl().deleteKVConfigValue(namespace, key, timeoutMillis);
    }


    @Override
    public String getProjectGroupByIp(String ip) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mqClientInstance.getMQClientAPIImpl().getProjectGroupByIp(ip, 3000);
    }

    @Override
    public String getProjectGroupByIp(String ip, long timeoutMillis) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mqClientInstance.getMQClientAPIImpl().getProjectGroupByIp(ip, timeoutMillis);
    }


    @Override
    public String getIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        return this.mqClientInstance.getMQClientAPIImpl().getKVConfigByValue(namespace, projectGroup, 3000);
    }

    @Override
    public String getIpsByProjectGroup(String projectGroup, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        return this.mqClientInstance.getMQClientAPIImpl().getKVConfigByValue(namespace, projectGroup, timeoutMillis);
    }


    @Override
    public void deleteIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        this.mqClientInstance.getMQClientAPIImpl().deleteKVConfigByValue(namespace, projectGroup, 3000);
    }

    @Override
    public void deleteIpsByProjectGroup(String projectGroup, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        this.mqClientInstance.getMQClientAPIImpl().deleteKVConfigByValue(namespace, projectGroup, timeoutMillis);
    }


    @Override
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
            boolean force) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force, 3000L);
    }

    @Override
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
                                                         boolean force, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        List<RollbackStats> rollbackStatsList = new ArrayList<RollbackStats>();
        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                // 根据 consumerGroup 查找对应的 mq
                ConsumeStats consumeStats =
                        this.mqClientInstance.getMQClientAPIImpl().getConsumeStats(addr, consumerGroup, timeoutMillis);

                // 根据 topic 过滤不需要的 mq
                for (Map.Entry<MessageQueue, OffsetWrapper> entry : consumeStats.getOffsetTable().entrySet()) {
                    MessageQueue queue = entry.getKey();
                    OffsetWrapper offsetWrapper = entry.getValue();
                    if (topic.equals(queue.getTopic())) {
                        // 根据 timestamp 查找对应的offset
                        long offset = this.mqClientInstance.getMQClientAPIImpl().searchOffset(addr, topic,
                                queue.getQueueId(), timestamp, timeoutMillis);
                        // 构建按时间回溯消费进度
                        RollbackStats rollbackStats = new RollbackStats();
                        rollbackStats.setBrokerName(bd.getBrokerName());
                        rollbackStats.setQueueId(queue.getQueueId());
                        rollbackStats.setBrokerOffset(offsetWrapper.getBrokerOffset());
                        rollbackStats.setConsumerOffset(offsetWrapper.getConsumerOffset());
                        rollbackStats.setTimestampOffset(offset);
                        rollbackStats.setRollbackOffset(offsetWrapper.getConsumerOffset());
                        // 更新 offset
                        if (force || offset <= offsetWrapper.getConsumerOffset()) {
                            rollbackStats.setRollbackOffset(offset);
                            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
                            requestHeader.setConsumerGroup(consumerGroup);
                            requestHeader.setTopic(topic);
                            requestHeader.setQueueId(queue.getQueueId());
                            requestHeader.setCommitOffset(offset);
                            this.mqClientInstance.getMQClientAPIImpl().updateConsumerOffset(addr, requestHeader, timeoutMillis);
                        }
                        rollbackStatsList.add(rollbackStats);
                    }
                }
            }
        }
        return rollbackStatsList;
    }


    @Override
    public KVTable getKVListByNamespace(String namespace) throws RemotingException, MQClientException,
            InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getKVListByNamespace(namespace, 5000);
    }

    @Override
    public KVTable getKVListByNamespace(String namespace, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().getKVListByNamespace(namespace, timeoutMillis);
    }


    @Override
    public void updateBrokerConfig(String brokerAddr, Properties properties) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException,
            InterruptedException, MQBrokerException {
        this.mqClientInstance.getMQClientAPIImpl().updateBrokerConfig(brokerAddr, properties, 5000);
    }

    @Override
    public void updateBrokerConfig(String brokerAddr, Properties properties, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            UnsupportedEncodingException, InterruptedException, MQBrokerException {
        this.mqClientInstance.getMQClientAPIImpl().updateBrokerConfig(brokerAddr, properties, timeoutMillis);
    }


    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String brokerAddress, String group, long timestamp,
            boolean isForce) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return resetOffsetByTimestamp(topic, brokerAddress, group, timestamp, isForce, 5000L);
    }

    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String brokerAddress, String group,
                                                          long timestamp, boolean isForce, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        Map<MessageQueue, Long> allOffsetTable = new HashMap<MessageQueue, Long>();
        if (null == brokerAddress || brokerAddress.trim().isEmpty()) {
            TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
            List<BrokerData> brokerDatas = topicRouteData.getBrokerDatas();
            if (brokerDatas != null) {
                for (BrokerData brokerData : brokerDatas) {
                    String addr = brokerData.selectBrokerAddr();
                    if (addr != null) {
                        Map<MessageQueue, Long> offsetTable =
                                this.mqClientInstance.getMQClientAPIImpl().invokeBrokerToResetOffset(addr, topic,
                                        group, timestamp, isForce, timeoutMillis);
                        if (offsetTable != null) {
                            allOffsetTable.putAll(offsetTable);
                        }
                    }
                }
            }
        } else {
            Map<MessageQueue, Long> offsetTable = this.mqClientInstance.getMQClientAPIImpl()
                    .invokeBrokerToResetOffset(brokerAddress, topic, group, timestamp, isForce, timeoutMillis);

            if (null != offsetTable) {
                allOffsetTable.putAll(offsetTable);
            }
        }

        return allOffsetTable;
    }


    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return getConsumeStatus(topic, group, clientAddr, 5000L);
    }

    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
        // 每个 broker 上有所有的 consumer 连接，故只需要在一个 broker 执行即可。
        if (brokerDataList != null && brokerDataList.size() > 0) {
            String addr = brokerDataList.get(0).selectBrokerAddr();
            if (addr != null) {
                return this.mqClientInstance.getMQClientAPIImpl().invokeBrokerToGetConsumerStatus(addr,
                        topic, group, clientAddr, timeoutMillis);
            }
        }
        return Collections.EMPTY_MAP;
    }


    public void createOrUpdateOrderConf(String key, String value, boolean isCluster)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        createOrUpdateOrderConf(key, value, isCluster, 3000L);
    }

    @Override
    public void createOrUpdateOrderConf(String key, String value, boolean isCluster, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        if (isCluster) {
            this.mqClientInstance.getMQClientAPIImpl().putKVConfigValue(
                    NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, key, value, timeoutMillis);
        }
        else {
            String oldOrderConfs = null;
            try {
                oldOrderConfs = this.mqClientInstance.getMQClientAPIImpl().getKVConfigValue(
                        NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, key, timeoutMillis);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // 添加或替换需要更新的 broker
            Map<String, String> orderConfMap = new HashMap<String, String>();
            if (!UtilAll.isBlank(oldOrderConfs)) {
                String[] oldOrderConfArr = oldOrderConfs.split(";");
                for (String oldOrderConf : oldOrderConfArr) {
                    String[] items = oldOrderConf.split(":");
                    orderConfMap.put(items[0], oldOrderConf);
                }
            }
            String[] items = value.split(":");
            orderConfMap.put(items[0], value);

            StringBuilder newOrderConf = new StringBuilder();
            String splitter = "";
            for (String tmp : orderConfMap.keySet()) {
                newOrderConf.append(splitter).append(orderConfMap.get(tmp));
                splitter = ";";
            }
            this.mqClientInstance.getMQClientAPIImpl().putKVConfigValue(
                    NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, key, newOrderConf.toString(), timeoutMillis);
        }
    }


    @Override
    public GroupList queryTopicConsumeByWho(String topic) throws InterruptedException, MQBrokerException,
            RemotingException, MQClientException {
        return queryTopicConsumeByWho(topic, 3000L);
    }

    @Override
    public GroupList queryTopicConsumeByWho(String topic, long timeoutMillis) throws InterruptedException,
            MQBrokerException, RemotingException, MQClientException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                return this.mqClientInstance.getMQClientAPIImpl().queryTopicConsumeByWho(addr, topic, timeoutMillis);
            }

            break;
        }

        return null;
    }


    @Override
    public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return queryConsumeTimeSpan(topic, group, 3000L);
    }

    @Override
    public Set<QueueTimeSpan> queryConsumeTimeSpan(String topic, String group, long timeoutMillis)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                return this.mqClientInstance.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, group, timeoutMillis);
            }

            break;
        }
        return null;
    }


    @Override
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        try {
            this.resetOffsetByTimestamp(topic, null, consumerGroup, timestamp, true);
        }
        catch (MQClientException e) {
            if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                this.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, true);
                return;
            }
            throw e;
        }
    }

    @Override
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            this.resetOffsetByTimestamp(topic, null, consumerGroup, timestamp, true, timeoutMillis);
        }
        catch (MQClientException e) {
            if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                this.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, true, timeoutMillis);
                return;
            }
            throw e;
        }
    }


    public boolean cleanExpiredConsumerQueueByCluster(ClusterInfo clusterInfo, String cluster)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            MQClientException, InterruptedException {
        boolean result = false;
        String[] addrs = clusterInfo.retrieveAllAddrByCluster(cluster);
        for (String addr : addrs) {
            result = cleanExpiredConsumerQueueByAddr(addr);
        }
        return result;
    }


    @Override
    public boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        boolean result = false;
        try {
            ClusterInfo clusterInfo = examineBrokerClusterInfo();
            if (null == cluster || "".equals(cluster)) {
                for (String targetCluster : clusterInfo.retrieveAllClusterNames()) {
                    result = cleanExpiredConsumerQueueByCluster(clusterInfo, targetCluster);
                }
            }
            else {
                result = cleanExpiredConsumerQueueByCluster(clusterInfo, cluster);
            }
        }
        catch (MQBrokerException e) {
            log.error("cleanExpiredConsumerQueue error.", e);
        }

        return result;
    }


    @Override
    public boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        boolean result = mqClientInstance.getMQClientAPIImpl().cleanExpiredConsumeQueue(addr, 3000L);
        log.warn("clean expired ConsumeQueue on target " + addr + " broker " + result);
        return result;
    }

    @Override
    public boolean cleanExpiredConsumerQueueByAddr(String addr, long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        boolean result = mqClientInstance.getMQClientAPIImpl().cleanExpiredConsumeQueue(addr, timeoutMillis);
        log.warn("clean expired ConsumeQueue on target " + addr + " broker " + result);
        return result;
    }


    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack)
            throws RemotingException, MQClientException, InterruptedException {
        return getConsumerRunningInfo(consumerGroup, clientId, jstack, 12000L);
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack,
                                                      long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        String topic = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(topic, timeoutMillis);
        List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
        if (brokerDataList != null) {
            for (BrokerData brokerData : brokerDataList) {
                String addr = brokerData.selectBrokerAddr();
                if (addr != null) {
                    return this.mqClientInstance.getMQClientAPIImpl().getConsumerRunningInfo(addr,
                            consumerGroup, clientId, jstack, timeoutMillis);
                }
            }
        }
        return null;
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId,
            String msgId) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException {
        return consumeMessageDirectly(consumerGroup, clientId, msgId, 10000L);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId, String msgId,
                                                               long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        MessageExt msg = this.viewMessage(msgId);

        return this.mqClientInstance.getMQClientAPIImpl().consumeMessageDirectly(
                RemotingUtil.socketAddress2String(msg.getStoreHost()), consumerGroup, clientId, msgId, timeoutMillis);
    }


    public boolean consumed(final MessageExt msg, final String group) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        return consumed(msg, group, 3000L);
    }

    public boolean consumed(final MessageExt msg, final String group, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        // 查询消费进度
        ConsumeStats consumeStats = this.examineConsumeStats(group, timeoutMillis);

        ClusterInfo ci = this.examineBrokerClusterInfo(timeoutMillis);

        for (Entry<MessageQueue, OffsetWrapper> next : consumeStats.getOffsetTable().entrySet()) {
            MessageQueue mq = next.getKey();
            if (mq.getTopic().equals(msg.getTopic()) && mq.getQueueId() == msg.getQueueId()) {
                BrokerData brokerData = ci.getBrokerAddrTable().get(mq.getBrokerName());
                if (brokerData != null) {
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr.equals(RemotingUtil.socketAddress2String(msg.getStoreHost()))) {
                        if (next.getValue().getConsumerOffset() > msg.getQueueOffset()) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }


    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return messageTrackDetail(msg, 3000L);
    }

    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<MessageTrack> result = new ArrayList<MessageTrack>();

        GroupList groupList = this.queryTopicConsumeByWho(msg.getTopic(), timeoutMillis);

        for (String group : groupList.getGroupList()) {
            // 查询连接
            MessageTrack mt = new MessageTrack();
            mt.setConsumerGroup(group);
            mt.setTrackType(TrackType.UNKNOWN_EXCEPTION);
            try {
                ConsumerConnection cc = this.examineConsumerConnectionInfo(group, timeoutMillis);
                switch (cc.getConsumeType()) {
                    case CONSUME_ACTIVELY:
                        mt.setTrackType(TrackType.SUBSCRIBED_BUT_PULL);
                        break;
                    case CONSUME_PASSIVELY:
                        boolean ifConsumed = this.consumed(msg, group, timeoutMillis);
                        if (ifConsumed) {
                            mt.setTrackType(TrackType.SUBSCRIBED_AND_CONSUMED);

                            // 查看订阅关系是否匹配
                            for (Entry<String, SubscriptionData> next : cc.getSubscriptionTable().entrySet()) {
                                if (next.getKey().equals(msg.getTopic())) {
                                    if (next.getValue().getTagsSet().contains(msg.getTags()) //
                                            || next.getValue().getTagsSet().contains("*")//
                                            || next.getValue().getTagsSet().isEmpty()//
                                            ) {
                                        //Already consumed.
                                    } else {
                                        mt.setTrackType(TrackType.SUBSCRIBED_BUT_FILTERD);
                                    }
                                }
                            }
                        }
                        else {
                            mt.setTrackType(TrackType.SUBSCRIBED_AND_NOT_CONSUME_YET);
                        }
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e) {
                mt.setExceptionDesc(RemotingHelper.exceptionSimpleDesc(e));
            }

            result.add(mt);
        }

        return result;
    }


    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        cloneGroupOffset(srcGroup, destGroup, topic, isOffline, 15000L);
    }

    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        String retryTopic = MixAll.getRetryTopic(srcGroup);
        TopicRouteData topicRouteData = this.examineTopicRouteInfo(retryTopic, timeoutMillis);

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                // 由于查询时间戳会产生IO操作，可能会耗时较长，所以超时时间设置为15s
                this.mqClientInstance.getMQClientAPIImpl().cloneGroupOffset(addr, srcGroup, destGroup, topic,
                        isOffline, timeoutMillis);
            }
        }
    }


    @Override
    public BrokerStatsData ViewBrokerStatsData(String brokerAddr, String statsName, String statsKey)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            MQClientException, InterruptedException {
        return this.mqClientInstance.getMQClientAPIImpl().ViewBrokerStatsData(brokerAddr, statsName, statsKey, 3000);
    }
}

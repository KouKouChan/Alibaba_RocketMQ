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

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.RollbackStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.tools.admin.api.MessageTrack;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * 所有运维接口都在这里实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class DefaultMQAdminExt extends ClientConfig implements MQAdminExt {
    private final DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private String adminExtGroup = "admin_ext_group";
    private String createTopicKey = MixAll.DEFAULT_TOPIC;


    public DefaultMQAdminExt() {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null);
    }


    public DefaultMQAdminExt(RPCHook rpcHook) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook);
    }


    public DefaultMQAdminExt(final String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public void start() throws MQClientException {
        defaultMQAdminExtImpl.start();
    }


    @Override
    public void shutdown() {
        defaultMQAdminExtImpl.shutdown();
    }


    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfig(addr, config);
    }

    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfig(addr, config, timeoutMillis);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfig(addr, config);
    }

    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfig(addr, config, timeoutMillis);
    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        return defaultMQAdminExtImpl.examineSubscriptionGroupConfig(addr, group);
    }


    @Override
    public SubscriptionGroupWrapper fetchAllSubscriptionGroups(String brokerAddress, long timeout) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.fetchAllSubscriptionGroups(brokerAddress, timeout);
    }

    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        return defaultMQAdminExtImpl.examineTopicConfig(addr, topic);
    }


    @Override
    public TopicStatsTable examineTopicStats(String topic) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(topic);
    }

    @Override
    public TopicStatsTable examineTopicStats(String topic, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(topic, timeoutMillis);
    }


    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(consumerGroup);
    }

    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(consumerGroup, timeoutMillis);
    }


    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        return defaultMQAdminExtImpl.examineBrokerClusterInfo();
    }

    @Override
    public ClusterInfo examineBrokerClusterInfo(long timeoutMillis) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        return defaultMQAdminExtImpl.examineBrokerClusterInfo(timeoutMillis);
    }


    @Override
    public TopicRouteData examineTopicRouteInfo(String topic) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.examineTopicRouteInfo(topic);
    }

    @Override
    public TopicRouteData examineTopicRouteInfo(String topic, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.examineTopicRouteInfo(topic, timeoutMillis);
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        defaultMQAdminExtImpl.putKVConfig(namespace, key, value);
    }


    @Override
    public String getKVConfig(String namespace, String key) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key);
    }

    @Override
    public String getKVConfig(String namespace, String key, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key, timeoutMillis);
    }


    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup);
    }

    @Override
    public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup, long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup, timeoutMillis);
    }


    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineProducerConnectionInfo(producerGroup, topic);
    }

    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup, String topic, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineProducerConnectionInfo(producerGroup, topic, timeoutMillis);
    }


    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.wipeWritePermOfBroker(namesrvAddr, brokerName);
    }

    @Override
    public int wipeWritePermOfBroker(String namesrvAddr, String brokerName, long timeoutMillis) throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.wipeWritePermOfBroker(namesrvAddr, brokerName, timeoutMillis);
    }


    public String getAdminExtGroup() {
        return adminExtGroup;
    }


    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }


    public String getCreateTopicKey() {
        return createTopicKey;
    }


    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.defaultMQAdminExtImpl.getNameServerAddressList();
    }


    @Override
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllTopicList();
    }

    @Override
    public TopicList fetchAllTopicList(long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllTopicList(timeoutMillis);
    }


    @Override
    public KVTable fetchBrokerRuntimeStats(final String brokerAddr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.fetchBrokerRuntimeStats(brokerAddr);
    }

    @Override
    public KVTable fetchBrokerRuntimeStats(String brokerAddr, long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.fetchBrokerRuntimeStats(brokerAddr, timeoutMillis);
    }


    @Override
    public void deleteTopicInBroker(Set<String> addrs, String topic) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInBroker(addrs, topic);
    }

    @Override
    public void deleteTopicInBroker(Set<String> addrs, String topic, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInBroker(addrs, topic, timeoutMillis);
    }


    @Override
    public void deleteTopicInNameServer(Set<String> addrs, String topic, String brokerAddresses)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, topic, brokerAddresses);
    }

    @Override
    public void deleteTopicInNameServer(Set<String> addrs, String topic, String brokerAddresses, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, topic, brokerAddresses, timeoutMillis);
    }


    @Override
    public void deleteSubscriptionGroup(String addr, String groupName) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName);
    }

    @Override
    public void deleteSubscriptionGroup(String addr, String groupName, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName, timeoutMillis);
    }


    @Override
    public void createAndUpdateKvConfig(String namespace, String key, String value) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateKvConfig(namespace, key, value);
    }

    @Override
    public void createAndUpdateKvConfig(String namespace, String key, String value, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateKvConfig(namespace, key, value, timeoutMillis);
    }


    @Override
    public void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteKvConfig(namespace, key);
    }

    @Override
    public void deleteKvConfig(String namespace, String key, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteKvConfig(namespace, key, timeoutMillis);
    }


    @Override
    public String getProjectGroupByIp(String ip) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getProjectGroupByIp(ip);
    }

    @Override
    public String getProjectGroupByIp(String ip, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getProjectGroupByIp(ip, timeoutMillis);
    }


    @Override
    public String getIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getIpsByProjectGroup(projectGroup);
    }

    @Override
    public String getIpsByProjectGroup(String projectGroup, long timeoutMillis) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getIpsByProjectGroup(projectGroup, timeoutMillis);
    }


    @Override
    public void deleteIpsByProjectGroup(String projectGroup) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteIpsByProjectGroup(projectGroup);
    }

    @Override
    public void deleteIpsByProjectGroup(String projectGroup, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deleteIpsByProjectGroup(projectGroup, timeoutMillis);
    }


    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
            boolean force) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
    }

    @Override
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
                                                         boolean force, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force, timeoutMillis);
    }


    @Override
    public KVTable getKVListByNamespace(String namespace) throws RemotingException, MQClientException,
            InterruptedException {
        return defaultMQAdminExtImpl.getKVListByNamespace(namespace);
    }

    @Override
    public KVTable getKVListByNamespace(String namespace, long timeoutMillis) throws RemotingException,
            MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVListByNamespace(namespace, timeoutMillis);
    }


    @Override
    public void updateBrokerConfig(String brokerAddr, Properties properties) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException,
            InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.updateBrokerConfig(brokerAddr, properties);
    }

    @Override
    public void updateBrokerConfig(String brokerAddr, Properties properties, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            UnsupportedEncodingException, InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.updateBrokerConfig(brokerAddr, properties, timeoutMillis);
    }


    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String brokerAddress, String group,
                                                          long timestamp, boolean isForce)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(topic, brokerAddress, group, timestamp, isForce);
    }

    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String brokerAddress, String group,
                                                          long timestamp, boolean isForce, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(topic, brokerAddress, group, timestamp, isForce, timeoutMillis);
    }


    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getConsumeStatus(topic, group, clientAddr);
    }

    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getConsumeStatus(topic, group, clientAddr, timeoutMillis);
    }


    @Override
    public void createOrUpdateOrderConf(String key, String value, boolean isCluster)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, isCluster);
    }

    @Override
    public void createOrUpdateOrderConf(String key, String value, boolean isCluster, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, isCluster, timeoutMillis);
    }


    @Override
    public GroupList queryTopicConsumeByWho(String topic) throws InterruptedException, MQBrokerException,
            RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicConsumeByWho(topic);
    }

    @Override
    public GroupList queryTopicConsumeByWho(String topic, long timeoutMillis) throws   InterruptedException,
            MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicConsumeByWho(topic, timeoutMillis);
    }


    @Override
    public Set<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group);
    }

    @Override
    public Set<QueueTimeSpan> queryConsumeTimeSpan(String topic, String group, long timeoutMillis)
            throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group, timeoutMillis);
    }


    @Override
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQAdminExtImpl.resetOffsetNew(consumerGroup, topic, timestamp);
    }

    @Override
    public void resetOffsetNew(String consumerGroup, String topic, long timestamp, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQAdminExtImpl.resetOffsetNew(consumerGroup, topic, timestamp, timeoutMillis);
    }


    @Override
    public boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueue(cluster);
    }


    @Override
    public boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr(addr);
    }

    @Override
    public boolean cleanExpiredConsumerQueueByAddr(String addr, long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr(addr, timeoutMillis);
    }


    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack)
            throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack,
                                                      long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack, timeoutMillis);
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId,
            String msgId) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, msgId);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId, String msgId,
                                                               long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, msgId, timeoutMillis);
    }


    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg) throws RemotingException, MQClientException,
            InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.messageTrackDetail(msg);
    }

    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return null;
    }


    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
    }

    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, topic, isOffline, timeoutMillis);
    }

    @Override
    public BrokerStatsData ViewBrokerStatsData(String brokerAddr, String statsName, String statsKey)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.ViewBrokerStatsData(brokerAddr, statsName, statsKey);
    }
}

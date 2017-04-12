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
package com.alibaba.rocketmq.client.consumer.rebalance;

import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.Pair;
import com.alibaba.rocketmq.common.constant.NSConfigKey;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;


/**
 * Allocate message queue by data center.
 *
 * @author Li Zhanhui
 * @since 1.0
 */
public class AllocateMessageQueueByDataCenter implements AllocateMessageQueueStrategy {

    private static final Logger LOGGER = ClientLogger.getLog();

    private MQClientInstance clientInstance;

    public AllocateMessageQueueByDataCenter(MQClientInstance clientInstance) {
        this.clientInstance = clientInstance;
    }

    /**
     * Name of this allocation algorithm.
     * @return Algorithm name.
     */
    @Override
    public String getName() {
        return "DATA_CENTER";
    }

    /**
     * <p>
     *    This method allocates message queue by data center.
     * </p>
     *
     * @param consumerGroup Consumer group.
     * @param currentConsumerID buffered consumer client ID, in form of IP@instance_name
     * @param mqAll 当前Topic的所有队列集合，无重复数据，且有序
     * @param allConsumerIDs All consumer IDs.
     * @return message queues allocated to current consumer client.
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentConsumerID, List<MessageQueue> mqAll,
            List<String> allConsumerIDs) {
        Helper.checkRebalanceParameters(consumerGroup, currentConsumerID, mqAll, allConsumerIDs);

        if (!allConsumerIDs.contains(currentConsumerID)) {
            LOGGER.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", //
                    consumerGroup, //
                    currentConsumerID,//
                    allConsumerIDs);
            return new ArrayList<MessageQueue>();
        }

        String suspendConsumerIPRanges = null;
        List<Pair<Long, Long>> ranges = null;
        try {
            KVTable kvTable = clientInstance.getMQClientAPIImpl().getKVListByNamespace("DC_SELECTOR", 3000);
            HashMap<String, String> configMap = kvTable.getTable();
            suspendConsumerIPRanges = configMap.get(NSConfigKey.DC_SUSPEND_CONSUMER_BY_IP_RANGE.getKey());
            if (null != suspendConsumerIPRanges && !suspendConsumerIPRanges.trim().isEmpty()) {
                ranges = MQHelper.buildIPRanges(suspendConsumerIPRanges);
            }
        } catch (InterruptedException | RemotingException e) {
            LOGGER.error("Error fetching suspended consumers", e);
        } catch (MQClientException e) {
            if (e.getMessage().contains("DC_SELECTOR")) {
                LOGGER.error(e.getMessage());
            } else {
                LOGGER.error("Error fetching suspended consumers", e);
            }
        }

        if (isSuspended(ranges, currentConsumerID)) {
            return new ArrayList<MessageQueue>();
        }

        //Filter out those suspended consumers.
        List<String> activeConsumerIds = new ArrayList<String>();
        for (String clientId : allConsumerIDs) {
            if (!isSuspended(ranges, clientId)) {
                activeConsumerIds.add(clientId);
            }
        }

        //This map holds final result.
        HashMap<String/*client-id*/, List<MessageQueue>> result = new HashMap<String, List<MessageQueue>>();

        //group message queues by data center.
        HashMap<Integer, List<MessageQueue>> groupedMessageQueues = new HashMap<Integer, List<MessageQueue>>();
        for (MessageQueue messageQueue : mqAll) {
            Integer dataCenterIndex = inferDataCenterByBrokerName(messageQueue.getBrokerName());
            if (!groupedMessageQueues.containsKey(dataCenterIndex)) {
                List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
                messageQueueList.add(messageQueue);
                groupedMessageQueues.put(dataCenterIndex, messageQueueList);
            } else {
                groupedMessageQueues.get(dataCenterIndex).add(messageQueue);
            }
        }

        //group active consumers by data center.
        HashMap<Integer, List<String>> groupedClients = new HashMap<Integer, List<String>>();
        for (String clientID : activeConsumerIds) {
            Integer dataCenterIndex = inferDataCenterByClientID(clientID);
            if (!groupedClients.containsKey(dataCenterIndex)) {
                List<String> clientIdList = new ArrayList<String>();
                clientIdList.add(clientID);
                groupedClients.put(dataCenterIndex, clientIdList);
            } else {
                groupedClients.get(dataCenterIndex).add(clientID);
            }
        }
        List<MessageQueue> pendingMessageQueues = new ArrayList<>();
        for (Map.Entry<Integer, List<MessageQueue>> next : groupedMessageQueues.entrySet()) {
            if (groupedClients.keySet().contains(next.getKey())) {
                allocateByCircle(next.getValue(), groupedClients.get(next.getKey()), result);
            } else {
                pendingMessageQueues.addAll(next.getValue());
            }
        }

        if (!pendingMessageQueues.isEmpty()) {
            allocateByCircle(pendingMessageQueues, activeConsumerIds, result);
        }

        List<MessageQueue> allocationResult = result.containsKey(currentConsumerID) ? result.get(currentConsumerID) : new ArrayList<MessageQueue>();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Algorithm: {}", getName());
            String topic = "Unknown";
            if (!mqAll.isEmpty()) {
                topic = mqAll.get(0).getTopic();
            }
            LOGGER.info("CG: {}, Topic: {}, Allocation Result:", consumerGroup, topic);
            for (MessageQueue messageQueue : allocationResult) {
                LOGGER.info("Broker Name: {}, Queue ID: {}", messageQueue.getBrokerName(), messageQueue.getQueueId());
            }
        }

        return allocationResult;
    }

    private static boolean isSuspended(List<Pair<Long, Long>> ranges, String consumerId) {
        if (null == ranges || ranges.isEmpty()) {
            return false;
        }
        if (consumerId.contains("@")) {
            String currentConsumerIP = consumerId.split("@")[0];
            Matcher matcher = MQHelper.IP_PATTERN.matcher(currentConsumerIP);
            if (matcher.matches()) {
                long currentConsumerIPInNumerical = MQHelper.ipAddressToNumber(currentConsumerIP);
                if (currentConsumerIPInNumerical > 0) {
                    for (Pair<Long, Long> range : ranges) {
                        if (range.getObject1() <= currentConsumerIPInNumerical
                                && range.getObject2() >= currentConsumerIPInNumerical) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private static void allocateByCircle(List<MessageQueue> messageQueues, List<String> clientIDs,
                                          HashMap<String, List<MessageQueue>> result) {
        for (int i = 0; i < messageQueues.size(); i++) {
            String targetClientId = clientIDs.get(i % clientIDs.size());
            if (!result.containsKey(targetClientId)) {
                List<MessageQueue> list = new ArrayList<>();
                list.add(messageQueues.get(i));
                result.put(targetClientId, list);
            } else {
                result.get(targetClientId).add(messageQueues.get(i));
            }
        }
    }

    private static int inferDataCenterByBrokerName(String brokerName) {
        Matcher matcher = Helper.BROKER_NAME_PATTERN.matcher(brokerName);
        if (!matcher.matches()) {
            return -1;
        } else {
            return Integer.parseInt(matcher.group(1));
        }
    }

    private static int inferDataCenterByClientID(String clientID) {
        if (null == clientID || !clientID.contains("@")) {
            return -1;
        }
        String clientIP = clientID.split("@")[0];

        Matcher matcher = MQHelper.IP_PATTERN.matcher(clientIP);

        if (!matcher.matches()) {
            return -1;
        } else {
            return Integer.parseInt(matcher.group(2));
        }
    }
}

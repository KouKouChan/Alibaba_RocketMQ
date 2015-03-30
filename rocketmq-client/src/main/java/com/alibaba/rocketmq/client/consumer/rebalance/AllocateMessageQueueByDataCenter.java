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

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
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

    private DefaultMQPushConsumer defaultMQPushConsumer;

    public AllocateMessageQueueByDataCenter(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
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
     * <p>
     *     Prerequisite:
     *     <ul>
     *         <li>Broker names conform to pattern of {@link Helper#BROKER_NAME_REGEX}</li>
     *         <li>Consumers use IPv4 address, whose second integer represents data center the very consumer reside in.
     *         </li>
     *     </ul>
     * </p>
     *
     * <p>
     *     Algorithm specification:
     *     <ul>
     *         <li>Filter out all suspended clients.</li>
     *         <li>For those DCs which have both consumers and brokers in, message queues are allocated averagely per DC.</li>
     *         <li>For those DCs which have brokers only, their message queues are allocated to all active consumers
     *         averagely. Note, under allocated consumers from previous step may allocate more message queues in this
     *         step, improving load balance.</li>
     *         <li>For those DCs which have consumers only, all their opportunities of allocation lie in the previous
     *         step. They indeed have risks of starvation.</li>
     *     </ul>
     * </p>
     *
     * @param consumerGroup Consumer group.
     * @param currentConsumerID concurrent consumer client ID, in form of IP@instance_name
     * @param mqAll
     *            当前Topic的所有队列集合，无重复数据，且有序
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
            KVTable kvTable = defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory()
                    .getMQClientAPIImpl().getKVListByNamespace("DC_SELECTOR", 3000);

            HashMap<String, String> configMap = kvTable.getTable();
            suspendConsumerIPRanges = configMap.get(NSConfigKey.DC_SUSPEND_CONSUMER_BY_IP_RANGE.getKey());
            if (null != suspendConsumerIPRanges && !suspendConsumerIPRanges.trim().isEmpty()) {
                ranges = buildIPRanges(suspendConsumerIPRanges);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Error fetching suspended consumers", e);
        } catch (RemotingException e) {
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
        HashMap<String, List<MessageQueue>> result = new HashMap<String, List<MessageQueue>>();

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

        List<String> underAllocatedClientIds = new ArrayList<String>();

        //Scenario: all client consumers have broker message queues in same DC.
        if (groupedMessageQueues.keySet().containsAll(groupedClients.keySet())) {
            //Averagely allocate message queues to consumers, both of which are of the same DC.
            for (Integer dcIndex : groupedClients.keySet()) {
                List<String> clientIDsPerDC = groupedClients.get(dcIndex);
                List<MessageQueue> messageQueuesPerDC = groupedMessageQueues.get(dcIndex);
                allocateMessageQueueClientPerDC(messageQueuesPerDC, clientIDsPerDC, underAllocatedClientIds,
                        result);
            }

            //allocate those message queues where there are no consumer clients.
            if (groupedClients.size() < groupedMessageQueues.size()) {
                List<MessageQueue> unallocatedMessageQueues = new ArrayList<MessageQueue>();

                for (Integer dcIndex : groupedMessageQueues.keySet()) {
                    if (!groupedClients.containsKey(dcIndex)) {
                        unallocatedMessageQueues.addAll(groupedMessageQueues.get(dcIndex));
                    }
                }

                int unallocatedMessageQueueIndex = 0;
                if (unallocatedMessageQueues.size() <= underAllocatedClientIds.size()) {
                    for (String clientID : underAllocatedClientIds) {
                        if (unallocatedMessageQueueIndex >= unallocatedMessageQueues.size()) {
                            break;
                        }
                        result.get(clientID).add(unallocatedMessageQueues.get(unallocatedMessageQueueIndex++));
                    }

                } else {
                    int avg = unallocatedMessageQueues.size() / activeConsumerIds.size();
                    int remaining = unallocatedMessageQueues.size() % activeConsumerIds.size();

                    //First, allocate the message queues to all active clients averagely.
                    for (String clientId : activeConsumerIds) {
                        for (int i = 0; i < avg; i++) {
                            result.get(clientId).add(unallocatedMessageQueues.get(unallocatedMessageQueueIndex++));
                        }
                    }

                    //allocate the remaining message queues to those under allocated clients.
                    if (remaining > 0) {
                        int average = remaining / underAllocatedClientIds.size();
                        int restOfRemaining = remaining % underAllocatedClientIds.size();
                        for (int i = 0; i < underAllocatedClientIds.size(); i++) {
                            String clientId = underAllocatedClientIds.get(i);
                            if (i < restOfRemaining) {
                                for (int j = 0; j < average + 1; j++) {
                                    result.get(clientId).add(unallocatedMessageQueues.get(unallocatedMessageQueueIndex++));
                                }
                            } else if (unallocatedMessageQueueIndex < unallocatedMessageQueues.size()) {
                                for (int j = 0; j < average; j++) {
                                    result.get(clientId).add(unallocatedMessageQueues.get(unallocatedMessageQueueIndex++));
                                }
                            }  else {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            //Allocate those DCs which have both consumer and broker in.
            for (Integer dcIndex : groupedClients.keySet()) {
                if (groupedMessageQueues.keySet().contains(dcIndex)) {
                    List<String> clientIDsPerDC = groupedClients.get(dcIndex);
                    List<MessageQueue> messageQueuesPerDC = groupedMessageQueues.get(dcIndex);
                    allocateMessageQueueClientPerDC(messageQueuesPerDC, clientIDsPerDC, underAllocatedClientIds,
                            result);
                }
            }

            List<String> clientIDsNotInBrokerDCs = new ArrayList<String>();
            for (Integer dcIndex : groupedClients.keySet()) {
                if (!groupedMessageQueues.containsKey(dcIndex)) {
                    clientIDsNotInBrokerDCs.addAll(groupedClients.get(dcIndex));
                }
            }

            List<MessageQueue> messageQueuesHasNoClients = new ArrayList<MessageQueue>();
            for (Integer dcIndex : groupedMessageQueues.keySet()) {
                if (!groupedClients.keySet().contains(dcIndex)) {
                    messageQueuesHasNoClients.addAll(groupedMessageQueues.get(dcIndex));
                }
            }

            //Allocate those broker message queues that have no consumers in to consumers that have no brokers in same
            // DC.
            if (!messageQueuesHasNoClients.isEmpty()) {
                allocateAveragely(messageQueuesHasNoClients, clientIDsNotInBrokerDCs, result);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Allocation Result:");
            for (Map.Entry<String, List<MessageQueue>> row : result.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (MessageQueue messageQueue : row.getValue()) {
                    stringBuilder.append(messageQueue.getBrokerName()).append(":").append(messageQueue.getQueueId())
                            .append(", ");
                }
                if (stringBuilder.length() > 2) {
                    LOGGER.debug(row.getKey() + " --> " + stringBuilder.substring(0, stringBuilder.length() - 2));
                }
            }
            LOGGER.debug("Allocation End.");
        }
        return null == result.get(currentConsumerID) ? new ArrayList<MessageQueue>()
                : result.get(currentConsumerID);
    }

    /**
     * <p>
     * Valid ranges may be a single IPv4 address or two IPv4 addresses joined by "-". If the latter form is used, we
     * assume the starting IP is smaller or equal to the ending IP. Multiple ranges are concatenated by semi-colon ";".
     * </p>
     *
     * <p>
     *     For example, the following ranges are all valid:
     *     <ul>
     *         <li>10.2.2.1</li>
     *         <li>10.1.36.10-10.1.36.14;10.2.2.1</li>
     *         <li>10.1.36.10-10.1.36.14;10.2.2.1-10.3.255.255</li>
     *     </ul>
     * </p>
     *
     * @param range ranges of IP address, conforming to the rules described above.
     * @return Numerical representation of the ranges list.
     */
    private List<Pair<Long, Long>> buildIPRanges(String range) {
        String[] ipRanges = range.split(";");
        List<Pair<Long, Long>> numericalIPRanges = new ArrayList<Pair<Long, Long>>();
        for (String ipRange : ipRanges) {
            if (ipRange.contains("-")) {
                String[] ipAddresses = ipRange.split("-");
                if (ipAddresses.length == 2) {
                    long ipStart = Helper.ipAddressToNumber(ipAddresses[0]);
                    long ipEnd = Helper.ipAddressToNumber(ipAddresses[1]);

                    if (ipStart > 0 && ipStart <= ipEnd) {
                        Pair<Long, Long> rangeItem = new Pair<Long, Long>(ipStart, ipEnd);
                        numericalIPRanges.add(rangeItem);
                    } else {
                        LOGGER.error("Starting range IP: {} is less than ending IP: {}", ipAddresses[0], ipAddresses[1]);
                    }
                }
            } else {
                long numericalIP = Helper.ipAddressToNumber(ipRange);
                if (numericalIP > 0) {
                    Pair<Long, Long> rangeItem = new Pair<Long, Long>(numericalIP, numericalIP);
                    numericalIPRanges.add(rangeItem);
                } else {
                    LOGGER.error("Ignoring mal-formed IP address: {}", ipRange);
                }
            }
        }

        return numericalIPRanges;

    }

    private static boolean isSuspended(List<Pair<Long, Long>> ranges, String consumerId) {
        if (null == ranges || ranges.isEmpty()) {
            return false;
        }
        if (consumerId.contains("@")) {
            String currentConsumerIP = consumerId.split("@")[0];
            Matcher matcher = Helper.IP_PATTERN.matcher(currentConsumerIP);
            if (matcher.matches()) {
                long currentConsumerIPInNumerical = Helper.ipAddressToNumber(currentConsumerIP);
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


    private static void allocateMessageQueueClientPerDC(List<MessageQueue> messageQueuesPerDC,
                                                 List<String> clientIDsPerDC,
                                                 List<String> underAllocatedClientIds,
                                                 HashMap<String, List<MessageQueue>> result) {
        int avgMessageQueuePerClient = messageQueuesPerDC.size() / clientIDsPerDC.size();
        int remaining = messageQueuesPerDC.size() % clientIDsPerDC.size();

        int messageQueueIndex = 0;
        for (int i = 0; i < clientIDsPerDC.size(); i++) {
            List<MessageQueue> resultItemList = new ArrayList<MessageQueue>();
            String clientId = clientIDsPerDC.get(i);
            if (i < remaining) {
                for (int j = 0; j < avgMessageQueuePerClient + 1; j++) {
                    resultItemList.add(messageQueuesPerDC.get(messageQueueIndex++));
                }
                result.put(clientIDsPerDC.get(i), resultItemList);
            } else if (messageQueueIndex < messageQueuesPerDC.size()) {
                for (int j = 0; j < avgMessageQueuePerClient; j++) {
                    resultItemList.add(messageQueuesPerDC.get(messageQueueIndex++));
                }
                result.put(clientId, resultItemList);
                underAllocatedClientIds.add(clientId);
            } else {
                result.put(clientId, resultItemList);
                underAllocatedClientIds.add(clientId);
            }
        }

    }

    private static void allocateAveragely(List<MessageQueue> messageQueues, List<String> clientIDs,
                                   HashMap<String, List<MessageQueue>> result) {
        int average = messageQueues.size() / clientIDs.size();
        int remain = messageQueues.size() % clientIDs.size();

        int messageQueueIndex = 0;
        for (int i = 0; i < clientIDs.size(); i++) {
            String clientId = clientIDs.get(i);
            List<MessageQueue> resultItemList = new ArrayList<MessageQueue>(average + 1);
            if (i < remain) {
                for (int j = 0; j < average + 1; j++) {
                    resultItemList.add(messageQueues.get(messageQueueIndex++));
                }
                result.put(clientId, resultItemList);
            } else if (messageQueueIndex < messageQueues.size()) {
                for (int j = 0; j < average; j++) {
                    resultItemList.add(messageQueues.get(messageQueueIndex++));
                }
                result.put(clientId, resultItemList);
            } else {
                break;
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

        Matcher matcher = Helper.IP_PATTERN.matcher(clientIP);

        if (!matcher.matches()) {
            return -1;
        } else {
            return Integer.parseInt(matcher.group(2));
        }
    }
}

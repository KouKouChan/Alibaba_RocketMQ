/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.Pair;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.constant.NSConfigKey;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 发送消息，根据数据中心选择队列
 */
public class SelectMessageQueueByDataCenter implements MessageQueueSelector {

    private static final Logger LOGGER = ClientLogger.getLog();

    private final Random random = new Random();

    private float locationRatio = 0.8f;

    private static final String DOCKER_DC_INDEX_ENV_KEY = "ROCKETMQ_DC_INDEX";

    private static final String DOCKER_DC_INDEX_KEY = "DCIndex";

    private String dispatchStrategy = "BY_LOCATION";

    private final AtomicInteger roundRobin = new AtomicInteger(0);

    private static String LOCAL_DATA_CENTER_ID = RemotingUtil.getLocalAddress(false).split("\\.")[1];

    private List<Pair<String, Float>> dispatcherList = new ArrayList<Pair<String, Float>>();

    private DefaultMQProducer defaultMQProducer;

    private AtomicLong warningCounter = new AtomicLong(0L);

    private static final long OUTPUT_WARNING_PER_COUNT = 1000L;

    private AtomicLong strategyWarnCounter = new AtomicLong(0L);

    static {
        String dcIndex = System.getenv(DOCKER_DC_INDEX_ENV_KEY);
        if (null == dcIndex) {
            dcIndex = System.getProperty(DOCKER_DC_INDEX_KEY);
        }

        if (null != dcIndex && dcIndex.trim().length() > 0) {
            LOCAL_DATA_CENTER_ID = dcIndex;
        }
    }

    public SelectMessageQueueByDataCenter(DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;
        startConfigUpdater();
    }

    private void startConfigUpdater() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (defaultMQProducer.getDefaultMQProducerImpl().getServiceState() != ServiceState.SHUTDOWN_ALREADY) {
                        try {
                            KVTable kvTable = getMQClientAPIImpl().getKVListByNamespace("DC_SELECTOR", 3000);
                            Map<String, String> configMap = kvTable.getTable();
                            String strategy = configMap.get(NSConfigKey.DC_DISPATCH_STRATEGY.getKey());

                            if ("BY_LOCATION".equals(strategy)) {
                                String locationRatio = configMap.get(NSConfigKey.DC_DISPATCH_STRATEGY_LOCATION_RATIO.getKey());
                                if (null == locationRatio || locationRatio.trim().isEmpty()) {
                                    dispatchStrategy = "AVERAGE";
                                    break;
                                }

                                try {
                                    SelectMessageQueueByDataCenter.this.locationRatio = Float.parseFloat(locationRatio);
                                    dispatchStrategy = strategy;
                                    LOGGER.info("Data center choosing strategy set to: " + dispatchStrategy);
                                    LOGGER.info("Fetched location ratio: " + locationRatio);
                                } catch (Exception e) {
                                    LOGGER.warn("DC_DISPATCH_STRATEGY_LOCATION_RATIO parse error: {}",
                                            SelectMessageQueueByDataCenter.this.locationRatio);
                                }
                            } else if ("BY_RATIO".equals(strategy)) {
                                String dispatchRatio = configMap.get(NSConfigKey.DC_DISPATCH_RATIO.getKey());
                                LOGGER.info("Fetched by-ratio values: " + dispatchRatio);
                                if (dispatchRatio != null && !dispatchRatio.trim().isEmpty()) {
                                    String[] values = dispatchRatio.split(",");
                                    List<Pair<String, Float>> newList = new ArrayList<Pair<String, Float>>();
                                    for (String value : values) {
                                        String keyValue[] = value.split(":");
                                        if (keyValue.length != 2) {
                                            LOGGER.warn("DC_DISPATCH_RATIO parse error: {}", dispatchRatio);
                                            continue;
                                        }
                                        Float dcRatio = null;
                                        try {
                                            dcRatio = Float.parseFloat(keyValue[1]);
                                        } catch (NumberFormatException e) {
                                            LOGGER.warn("DC_DISPATCH_RATIO parse error: {}", dispatchRatio);
                                            continue;
                                        }

                                        if (null != dcRatio && dcRatio > 0) {
                                            newList.add(new Pair<String, Float>(keyValue[0], dcRatio));
                                        }
                                    }

                                    Collections.sort(newList, new Comparator<Pair<String, Float>>() {

                                        @Override
                                        public int compare(Pair<String, Float> o1, Pair<String, Float> o2) {
                                            return o2.getObject2().compareTo(o1.getObject2());
                                        }
                                    });

                                    //Convert percent to percentile.
                                    if (newList.size() > 0) {
                                        for (int i = 1; i < newList.size(); i++) {
                                            Pair<String, Float> pair = newList.get(i);
                                            pair.setObject2(pair.getObject2() + newList.get(i - 1).getObject2());
                                        }

                                        List<Pair<String, Float>> tmpList = getDispatcherList();
                                        setDispatcherList(newList);
                                        tmpList.clear();
                                        dispatchStrategy = strategy;
                                        LOGGER.info("Data center choosing strategy set to: " + dispatchStrategy);
                                        LOGGER.info("Data center percentile as follows:");
                                        for (Pair<String, Float> item : getDispatcherList()) {
                                            LOGGER.info(item.getObject1() + " --> " + item.getObject2());
                                        }
                                    }
                                } else {
                                    dispatchStrategy = "AVERAGE";
                                }
                            } else {
                                dispatchStrategy = "AVERAGE";
                                LOGGER.warn("Unknown data center choosing strategy.");
                            }
                        } catch (Exception e) {
                            dispatchStrategy = "AVERAGE";
                            if (e.getMessage().contains("DC_SELECTOR")) {
                                LOGGER.error(e.getMessage());
                            } else {
                                LOGGER.error("Failed to select message queue to send message.", e);
                            }
                        }
                        //Sleep 60 seconds per loop, be it successful or not.
                        Thread.sleep(60 * 1000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "UpdateDCDispatchRatioThread-").start();

    }

    /**
     * This method always return a message queue for producer to send message. Exceptions are strictly prohibited.
     *
     * TODO Use <code>arg</code> to support order message.
     * @param mqs All available message queues.
     * @param msg message to send.
     * @param arg It may be null or instance of {@link com.alibaba.rocketmq.client.producer.SendCallback}.
     * @return Message queue to send the message.
     */
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        try {
            float r = random.nextFloat();
            List<MessageQueue> dataCenterQueues = new ArrayList<MessageQueue>();
            if ("BY_LOCATION".equals(dispatchStrategy)) {
                for (MessageQueue messageQueue : mqs) {

                    /**
                     * Broker name pattern: ClusterName_{DataCenterNumber}_broker{BrokerNumber}[_optional_extra_info]
                     * Sample broker name: DefaultCluster_1_broker1
                     */
                    String[] brokerNameSegments = messageQueue.getBrokerName().split("_");
                    if (3 > brokerNameSegments.length) {
                        warningCounter.incrementAndGet();
                        if (1 == warningCounter.longValue() || warningCounter.longValue() % OUTPUT_WARNING_PER_COUNT == 0) {
                            LOGGER.warn("Issue: broker name is not properly named. Check " + messageQueue.getBrokerName());
                        }
                        //Round-robin all message queues as broker name is not properly named.
                        break;
                    }

                    if (r > locationRatio && !brokerNameSegments[1].equals(LOCAL_DATA_CENTER_ID)) {
                        dataCenterQueues.add(messageQueue);
                    } else if (r <= locationRatio && brokerNameSegments[1].equals(LOCAL_DATA_CENTER_ID)) {
                        dataCenterQueues.add(messageQueue);
                    }
                }
                return roundRobin(dataCenterQueues, mqs);
            } else if ("BY_RATIO".equals(dispatchStrategy)) {
                List<Pair<String, Float>> list = getDispatcherList();
                String dc = list.get(0).getObject1();
                for (Pair<String, Float> item : list) {
                    if (r <= item.getObject2()) {
                        dc = item.getObject1();
                        break;
                    }
                }

                for (MessageQueue messageQueue : mqs) {
                    String[] brokerNameSegments = messageQueue.getBrokerName().split("_");
                    if (brokerNameSegments[1].equals(dc)) {
                        dataCenterQueues.add(messageQueue);
                    }
                }
                return roundRobin(dataCenterQueues, mqs);
            } else {

                if (strategyWarnCounter.incrementAndGet() % 10000 == 1) {
                    LOGGER.warn("Consume averagely, please double check.");
                }

                return roundRobin(mqs, mqs);
            }
        } catch (Exception e) {
            // Round robin all message queues, namely, average consuming in case of any error.
            return roundRobin(mqs, mqs);
        }
    }

    /**
     * This method first tries to round robin <code>preferableMessageQueues</code> in case it's not null nor empty.
     * If previous step fails, it tries to round robin all message queues specified by <code>availableMessageQueues</code>.
     * @param preferableMessageQueues Preferable message queues.
     * @param availableMessageQueues All message queues available.
     * @return The chosen message queue.
     */
    private MessageQueue roundRobin(List<MessageQueue> preferableMessageQueues, List<MessageQueue> availableMessageQueues) {
        if (null == preferableMessageQueues || preferableMessageQueues.isEmpty()) {
            return availableMessageQueues.get(roundRobin.incrementAndGet() % availableMessageQueues.size());
        }
        return preferableMessageQueues.get(roundRobin.incrementAndGet() % preferableMessageQueues.size());
    }

    public List<Pair<String, Float>> getDispatcherList() {
        return dispatcherList;
    }

    public void setDispatcherList(List<Pair<String, Float>> dispatcherList) {
        this.dispatcherList = dispatcherList;
    }

    private MQClientAPIImpl getMQClientAPIImpl() {
        return this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl();
    }

}

package com.alibaba.rocketmq.client.producer.filter;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueFilter;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.constant.NSConfigKey;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FilterMessageQueueByBroker
 * User: penuel (penuel.leo@gmail.com)
 * Date: 15/8/11 下午12:01
 * Desc:
 */

public class FilterMessageQueueByBroker implements MessageQueueFilter {

    private static final Logger LOGGER = ClientLogger.getLog();

    private DefaultMQProducer defaultMQProducer;

    /* brokerName List*/
    private AtomicReference<List<String>> matchedBrokerList = new AtomicReference<List<String>>();

    private AtomicReference<List<String>> matchedDataCenterIdList = new AtomicReference<List<String>>();

    /* type 0=NOFILTER 1=BYBROKER 2=BYDC*/
    private AtomicInteger filterType = new AtomicInteger(0);

    public FilterMessageQueueByBroker(DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;
    }

    private void initAndStartTimer() {
        LOGGER.debug("FilterMessageQueueByBroker initAndStartTimer...");
        new Timer().schedule(new TimerTask() {

            @Override public void run() {
                if ( defaultMQProducer.getDefaultMQProducerImpl().getServiceState() == ServiceState.SHUTDOWN_ALREADY ) {
                    return;
                }
                try {
                    KVTable kvTable = getMQClientAPIImpl().getKVListByNamespace(NamesrvUtil.NAMESPACE_PRODUCER_GROUP_FILTER_CONFIG, 3000);
                    Map<String, String> configMap = kvTable.getTable();
                    JSONObject producerGroupConfig = JSONObject.parseObject(configMap.get(defaultMQProducer.getProducerGroup()));
                    String brokers = producerGroupConfig.getString(NSConfigKey.PG_MQ_FILETER_BY_BROKER.getKey());
                    if ( null != brokers && 0 < brokers.length() ) {
                        matchedBrokerList.set(Arrays.asList(brokers.split(",")));
                        filterType.set(1);
                    } else {
                        String dcIDs = producerGroupConfig.getString(NSConfigKey.PG_MQ_FILETER_BY_DC.getKey());
                        matchedDataCenterIdList.set(Arrays.asList(dcIDs.split(",")));
                        filterType.set(2);
                    }
                } catch ( RemotingException e ) {
                    LOGGER.error("Failed to start updater for message queue filter.", e);
                } catch ( MQClientException e ) {
                    e.printStackTrace();
                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                }

            }
        }, 10 * 1000, 60 * 1000);

    }

    @Override
    public List<MessageQueue> filter(List<MessageQueue> mqs, Object arg) {
        switch ( filterType.get() ) {
        case 1:
            return filterByBrokerIds(mqs);
        case 2:
            return filterByDataCenter(mqs);
        default:
            return mqs;
        }
    }

    private List<MessageQueue> filterByDataCenter(List<MessageQueue> mqs) {
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        List<String> matchedDcIds = matchedDataCenterIdList.get();
        String[] brokerNameSegments;
        for ( MessageQueue mq : mqs ) {
            brokerNameSegments = mq.getBrokerName().split("_");
            if ( brokerNameSegments.length > 2 && matchedDcIds.contains(brokerNameSegments[1]) ) {
                result.add(mq);
            }
        }
        return defaultValueIfResultEmpty(result, mqs);
    }

    private List<MessageQueue> filterByBrokerIds(List<MessageQueue> mqs) {
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        List<String> matchedBrokers = matchedBrokerList.get();
        for ( MessageQueue mq : mqs ) {
            if ( matchedBrokers.contains(mq.getBrokerName()) ) {
                result.add(mq);
            }
        }
        return defaultValueIfResultEmpty(result, mqs);
    }

    private List<MessageQueue> defaultValueIfResultEmpty(List<MessageQueue> result, List<MessageQueue> defaultValue) {
        if ( null == result || result.isEmpty() ) {
            return defaultValue;
        }
        return result;
    }

    private MQClientAPIImpl getMQClientAPIImpl() {
        return this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl();
    }

}
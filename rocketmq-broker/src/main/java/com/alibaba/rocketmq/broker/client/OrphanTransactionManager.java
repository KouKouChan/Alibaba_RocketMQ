/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.alibaba.rocketmq.broker.client;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OrphanTransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    private final BrokerController brokerController;

    public OrphanTransactionManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void handleOrphanTransaction() {
        LOGGER.info("Start to handle orphan transactions");
        Map<String, Set<Long>> orphanTransactions = brokerController.getJdbcTransactionStore().getLaggedTransaction();
        if (null == orphanTransactions || orphanTransactions.isEmpty()) {
            LOGGER.info("No orphan transactions found.");
            return;
        } else {
            LOGGER.info("Found {} orphan transactions", orphanTransactions.size());
        }

        HashMap<String, HashMap<Channel, ClientChannelInfo>> groupChannelTable = brokerController.getProducerManager()
                .getGroupChannelTable();
        for (Map.Entry<String, Set<Long>> next : orphanTransactions.entrySet()) {
            if (!groupChannelTable.containsKey(next.getKey())) {
                LOGGER.warn("ProducerGroup: {} has no producer instances online.", next.getKey());
                continue;
            }

            Set<Long> offsets = next.getValue();
            final HashMap<Channel, ClientChannelInfo> clientChannelMap = groupChannelTable.get(next.getKey());
            for (final Long offset : offsets) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        SelectMapedBufferResult selectMapedBufferResult = brokerController.getMessageStore()
                                .selectOneMessageByOffset(offset);

                        //Select a channel randomly.
                        if (clientChannelMap.isEmpty()) {
                            return;
                        }
                        Channel channel = clientChannelMap.keySet().iterator().next();

                        ClientChannelInfo clientChannelInfo = clientChannelMap.get(channel);
                        CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
                        requestHeader.setCommitLogOffset(offset);
                        MessageExt messageExt = MessageDecoder.decode(selectMapedBufferResult.getByteBuffer());
                        requestHeader.setMsgId(messageExt.getMsgId());

                        //The following two fields are no longer used. Set for compatible purpose only.
                        requestHeader.setTranStateTableOffset(-1L);
                        requestHeader.setTransactionId("NO-LONGER-USED");

                        LOGGER.info("check producer transaction state against Producer ID: {}, Remoting Address: {}, for Message ID: {}",
                                clientChannelInfo.getClientId(),
                                clientChannelInfo.getChannel().remoteAddress(),
                                messageExt.getMsgId());
                        brokerController.getBroker2Client().checkProducerTransactionState(clientChannelInfo.getChannel(), requestHeader, selectMapedBufferResult);
                        LOGGER.info("Check transaction state request sent to {}.", clientChannelInfo.getChannel().remoteAddress());

                    }
                };
                brokerController.getBroker2ClientExecutorService().submit(runnable);
            }
        }

        LOGGER.info("Broker2Client tasks submitted");
    }

}

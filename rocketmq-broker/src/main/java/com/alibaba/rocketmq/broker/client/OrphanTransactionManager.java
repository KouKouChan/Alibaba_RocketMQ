/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.alibaba.rocketmq.broker.client;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OrphanTransactionManager {

    private final BrokerController brokerController;

    public OrphanTransactionManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void handleOrphanTransaction() {
        Map<String, Set<Long>> laggedTransactions = brokerController.getJdbcTransactionStore().getLaggedTransaction();
        if (null == laggedTransactions || laggedTransactions.isEmpty()) {
            return;
        }

        HashMap<String, HashMap<Channel, ClientChannelInfo>> groupChannelTable = brokerController.getProducerManager().getGroupChannelTable();

        for (Map.Entry<String, Set<Long>> next : laggedTransactions.entrySet()) {

            if (!groupChannelTable.containsKey(next.getKey())) { // All producer instances belonging to this group are dead.
                continue;
            }

            Set<Long> offsets = next.getValue();
            HashMap<Channel, ClientChannelInfo> clientChannelMap = groupChannelTable.get(next.getKey());

            for (Long offset : offsets) {
                SelectMapedBufferResult selectMapedBufferResult = brokerController.getMessageStore().selectOneMessageByOffset(offset);
                //Select a channel randomly.
                Channel channel = clientChannelMap.keySet().iterator().next();
                CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
                requestHeader.setCommitLogOffset(offset);
                MessageExt messageExt = MessageDecoder.decode(selectMapedBufferResult.getByteBuffer());
                requestHeader.setMsgId(messageExt.getMsgId());

                //The following two fields are no longer used. Set for compatible purpose only.
                requestHeader.setTranStateTableOffset(-1L);
                requestHeader.setTransactionId("NO-LONGER-USED");

                brokerController.getBroker2Client().checkProducerTransactionState(channel, requestHeader, selectMapedBufferResult);
            }
        }


    }

}

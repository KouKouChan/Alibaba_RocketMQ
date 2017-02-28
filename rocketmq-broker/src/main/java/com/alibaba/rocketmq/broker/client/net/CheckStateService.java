/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.broker.client.net;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.SelectMappedBufferResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckStateService implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    public static final int BATCH_SIZE = 10;

    private final BrokerController brokerController;

    public CheckStateService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void run() {
        boolean complete = false;
        long min = brokerController.getTransactionStore().minPK();
        long max = brokerController.getTransactionStore().maxPK();
        long offset = min;
        while (!complete) {
            if (offset > max) {
                complete = true;
                continue;
            }

            List<TransactionRecord> transactionRecords = brokerController.getTransactionStore().traverse(offset, BATCH_SIZE);
            if (transactionRecords.isEmpty()) {
                complete = true;
                continue;
            }

            for (TransactionRecord transactionRecord : transactionRecords) {
                ClientChannelInfo clientChannelInfo = brokerController.getProducerManager().pickProducerChannelRandomly(transactionRecord.getProducerGroup());
                if (null != clientChannelInfo) {
                    CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
                    requestHeader.setCommitLogOffset(transactionRecord.getOffset());
                    requestHeader.setTranStateTableOffset(1L);

                    // Retrieve the prepared message.
                    SelectMappedBufferResult selectMappedBufferResult = brokerController.getMessageStore()
                        .selectOneMessageByOffset(offset);

                    brokerController.getBroker2Client().checkProducerTransactionState(clientChannelInfo.getChannel(), requestHeader, selectMappedBufferResult);
                    offset = transactionRecord.getOffset() + 1;
                } else {
                    LOGGER.warn("There is no online producer instance of producer group: {}", transactionRecord.getProducerGroup());
                }
            }
        }
    }
}

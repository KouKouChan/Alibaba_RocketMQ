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
package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * 未决事务，服务器回查客户端
 */
public class TransactionCheckListenerImpl implements TransactionCheckListener {
    private AtomicInteger transactionIndex = new AtomicInteger(0);


    @Override
    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        System.out.println("server checking TrMsg " + msg.toString());
        transactionIndex.incrementAndGet();
        if (transactionIndex.get() % 3 == 0) {
            System.out.println("Commit the lagged TX message");
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (transactionIndex.get() % 3 == 1) {
            System.out.println("Rollback the lagged TX message");
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        System.out.println("Still Unknown to the lagged TX message.");
        return LocalTransactionState.UNKNOW;

    }
}

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
package com.alibaba.rocketmq.example.simple;

import java.util.Set;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * PullConsumer，订阅消息
 */
public class PullConsumer {


    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("CG_BI_YM_EVENT");
        consumer.registerMessageQueueListener("T_YM_EVENT", new TestMessageQueueListener(consumer));
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.start();

        Thread.sleep(Integer.MAX_VALUE);

        consumer.shutdown();
    }


    static class TestMessageQueueListener implements MessageQueueListener {

        private final DefaultMQPullConsumer pullConsumer;

        TestMessageQueueListener(DefaultMQPullConsumer pullConsumer) {
            this.pullConsumer = pullConsumer;
        }

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            for (MessageQueue messageQueue : mqAll) {
                boolean update = false;
                try {
                    long consumeOffset = pullConsumer.fetchConsumeOffset(messageQueue, true);
                    System.out.println("consume offset: " + consumeOffset);
                    if (consumeOffset < 0) {
                        update = true;
                    }
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                try {
                    long min = pullConsumer.minOffset(messageQueue);
                    System.out.println("min: " + min);
                    if (update) {
                        pullConsumer.getOffsetStore().updateOffset(messageQueue, min, true);
                        pullConsumer.getOffsetStore().persist(messageQueue);
                    }
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                try {
                    System.out.println("max: " + pullConsumer.maxOffset(messageQueue));
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

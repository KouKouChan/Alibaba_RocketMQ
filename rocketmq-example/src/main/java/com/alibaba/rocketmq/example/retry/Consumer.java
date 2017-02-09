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

package com.alibaba.rocketmq.example.retry;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import java.util.List;
import java.util.Set;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        if (args.length != 1) {
            showUsage();
            return;
        }

        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(args[0]);
        pullConsumer.start();
        Set<MessageQueue> messageQueueSet = pullConsumer.fetchSubscribeMessageQueues(MixAll.RETRY_GROUP_TOPIC_PREFIX + args[0]);
        for (MessageQueue messageQueue : messageQueueSet) {
            long min = pullConsumer.minOffset(messageQueue);
            long max = pullConsumer.maxOffset(messageQueue);
            System.out.println("There are at most" + (max - min) + " messages in " + messageQueue);
            long offset = min;
            boolean completed = false;
            while(!completed) {
                try {
                    PullResult pullResult = pullConsumer.pull(messageQueue, "*", offset, 32);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                            for (MessageExt messageExt : msgFoundList) {
                                System.out.println(messageExt);
                            }
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            completed = true;
                            break;
                        case OFFSET_ILLEGAL:
                            System.out.println("Offset Illegal");
                            break;
                        case SLAVE_LAG_BEHIND:
                            System.out.println("Slave lags behind");
                            break;

                        case SUBSCRIPTION_NOT_LATEST:
                            System.out.println("subscription not the latest");
                            break;
                        default:
                            System.out.println("Unknown pull result status");
                            break;
                    }
                    offset = pullResult.getNextBeginOffset();
                } catch (RemotingException e) {
                    e.printStackTrace();
                } catch (MQBrokerException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void showUsage() {
        System.out.println("java Consumer GROUP_NAME");
    }
}

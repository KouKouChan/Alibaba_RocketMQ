/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.example.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CountDownLatch;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("PG_QuickStart");
        producer.start();
        int total = 1000000;
        int tpsThrottle = 1000;

        if (args.length > 0) {
            tpsThrottle = Integer.parseInt(args[0]);
        }

        final CountDownLatch countDownLatch = new CountDownLatch(total);
        RateLimiter rateLimiter = RateLimiter.create(tpsThrottle);
        for (int i = 0; i < total; i++) {
            try {
                rateLimiter.acquire();
                Message msg = new Message("TopicTest",// topic
                        "TagA",// tag
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)// body
                );
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                        countDownLatch.countDown();
                    }
                });
                System.out.println("sent");
            } catch (Exception e) {
                countDownLatch.countDown();
                System.out.println(e.getMessage());
            }
        }

        countDownLatch.await();

        producer.shutdown();
    }
}

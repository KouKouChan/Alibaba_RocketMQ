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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {
    static volatile boolean stopped = false;

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("PG_QuickStart");
        producer.start();
        int total = 1000000;
        int tpsThrottle = 1000;

        if (args.length > 0) {
            tpsThrottle = Integer.parseInt(args[0]);
        }

        final AtomicLong ssc = new AtomicLong(0);
        final AtomicLong prevSSC = new AtomicLong(0);

        final AtomicLong fsc = new AtomicLong(0);
        final AtomicLong prevFSC = new AtomicLong(0);

        ScheduledExecutorService statService = Executors.newSingleThreadScheduledExecutor();
        statService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long sscCurrent = ssc.get();
                long fscCurrent = fsc.get();
                long sscDiff = sscCurrent - prevSSC.get();
                long fscDiff = fscCurrent - prevFSC.get();
                prevSSC.set(sscCurrent);
                prevFSC.set(fscCurrent);
                System.out.println("Success TPS: " + sscDiff / 60);
                System.out.println("Failure TPS: " + fscDiff / 60);
            }
        }, 1, 1, TimeUnit.MINUTES);

        RateLimiter rateLimiter = RateLimiter.create(tpsThrottle);
        while (!stopped) {
            try {
                rateLimiter.acquire();
                Message msg = new Message("TopicTest",// topic
                        "TagA",// tag
                        ("Hello RocketMQ").getBytes(RemotingHelper.DEFAULT_CHARSET)// body
                );
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        ssc.incrementAndGet();
                    }

                    @Override
                    public void onException(Throwable e) {
                        if (e instanceof InterruptedException) {
                            stopped = true;
                        }

                        fsc.incrementAndGet();
                    }
                });
            } catch (Exception e) {

                if (e instanceof InterruptedException) {
                    stopped = true;
                }

                fsc.incrementAndGet();
            }
        }

        producer.shutdown();
    }
}

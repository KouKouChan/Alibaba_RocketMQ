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
package com.alibaba.rocketmq.example.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.selector.Region;
import com.alibaba.rocketmq.client.producer.selector.SelectMessageQueueByRegion;
import com.alibaba.rocketmq.common.message.Message;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Arrays;


/**
 * ExampleCacheableProducer，发送消息
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("PG_QuickStart");
        producer.setSendMsgTimeout(10000);
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        float limit = 5000;
        if (args.length > 0) {
            limit = Float.parseFloat(args[0]);
        }
        Message msg = new Message("T_QuickStart",// topic
                "TagA",// tag
                data
        );
        msg.setWaitStoreMsgOK(false);
        RateLimiter rateLimiter = RateLimiter.create(limit);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            rateLimiter.acquire();
            try {
//                SendResult sendResult = producer.send(msg);
//                System.out.println(sendResult);
                producer.send(msg, new SelectMessageQueueByRegion(Region.ANY), null, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                }, 200, true);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}

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
import com.alibaba.rocketmq.common.message.Message;

import java.util.Arrays;


/**
 * ExampleCacheableProducer，发送消息
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("PG_QuickStart");
        producer.setSendMsgTimeout(10000);
        producer.start();

        byte[] data = new byte[2048];
        Arrays.fill(data, (byte)'x');
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try {
                Message msg = new Message("T_QuickStart",// topic
                        "TagA",// tag
                        data// body
                );
                producer.sendOneWay(msg);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}

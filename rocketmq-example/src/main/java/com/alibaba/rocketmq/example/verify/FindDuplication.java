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

package com.alibaba.rocketmq.example.verify;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FindDuplication {
    public static void main(String[] args) throws MQClientException, IOException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("FindDuplication");
        producer.start();

        File file = new File(args[0]);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = null;
        Set<String> msgIds = new HashSet<>();
        while (null != (line = bufferedReader.readLine())) {
            if (msgIds.contains(line.trim())) {
                System.out.println("Found duplicated msgIds");
            } else {
                msgIds.add(line.trim());
            }
        }

        Map<String, List<String>> keyIds = new HashMap<>();
        for (String msgId : msgIds) {
            MessageExt message = producer.viewMessage(msgId);
            if (null != message) {
                if (keyIds.containsKey(message.getKeys())) {
                    keyIds.get(message.getKeys()).add(msgId);
                } else {
                    keyIds.put(message.getKeys(), Arrays.asList(msgId));
                }
            }
        }

        System.out.println("Duplication Result:");
        for (Map.Entry<String, List<String>> next : keyIds.entrySet()) {
            if (next.getValue().size() > 1) {
                System.out.println(next.getKey() + ": " + next.getValue());
            }
        }

        producer.shutdown();
    }
}

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

package com.alibaba.rocketmq.client.common;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadLocalIndex {

    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();

    public ThreadLocalIndex() {
        threadLocalIndex.set(0);
    }

    public ThreadLocalIndex(int value) {
        threadLocalIndex.set(value < 0 ? 0 : value);
    }

    public ThreadLocalIndex(boolean rand) {
        if (rand) {
            Random random = new Random(System.currentTimeMillis());
            threadLocalIndex.set(Math.abs(random.nextInt()));
        } else {
            threadLocalIndex.set(0);
        }
    }

    public int getAndIncrement() {
        Integer index = this.threadLocalIndex.get();
        int ret = index;
        this.threadLocalIndex.set(++index < 0 ? 0 : index);
        return ret;
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.get() +
                '}';
    }
}

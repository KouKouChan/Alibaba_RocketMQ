package com.alibaba.rocketmq.example.smoking;

import com.alibaba.rocketmq.client.store.DefaultLocalMessageStore;
import com.alibaba.rocketmq.client.store.LocalMessageStore;
import com.alibaba.rocketmq.common.message.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalStore {
    private static volatile int prev;

    public static void main(String[] args) throws IOException {
        final LocalMessageStore localMessageStore = new DefaultLocalMessageStore("Test");
        localMessageStore.start();
        int parallelism = 4;
        final AtomicInteger count = new AtomicInteger();

        final byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        final Message message = new Message("TestTopic", data);
        final AtomicInteger round = new AtomicInteger(5);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(parallelism);
        for (int i = 0; i < parallelism - 1; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while (round.get() > 0) {
                        try {
                            localMessageStore.stash(message);
                            count.incrementAndGet();
                        } catch (Exception ignore) {
                        }
                    }
                }
            });
        }

        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                int stopWatch = count.get();
                System.out.println("QPS: " + (stopWatch - prev) / 30);
                prev = stopWatch;
                if (round.decrementAndGet() < 0) {
                    localMessageStore.close();
                }
            }
        }, 10, TimeUnit.SECONDS);
    }
}

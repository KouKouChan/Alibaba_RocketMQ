package com.alibaba.rocketmq.common;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class ShutdownHookTask implements Runnable {
    private final Logger log;
    private final ServerController controller;

    private volatile boolean hasShutdown = false;
    private AtomicInteger shutdownTimes = new AtomicInteger(0);

    public ShutdownHookTask(Logger log, ServerController controller) {
        this.log = log;
        this.controller = controller;
    }

    @Override
    public void run() {
        synchronized (this) {
            log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long beginTime = System.currentTimeMillis();
                controller.shutdown();
                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
            }
        }
    }
}

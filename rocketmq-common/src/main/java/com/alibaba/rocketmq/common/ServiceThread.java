/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 后台服务线程基类
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public abstract class ServiceThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.CommonLoggerName);

    // 执行线程
    protected final Thread thread;

    // 线程回收时间，默认90S
    private static final long JoinTime = 90 * 1000;

    // 是否已经被Notify过
    protected AtomicBoolean hasNotified = new AtomicBoolean(false);

    protected final CountDownLatch latch;

    // 线程是否已经停止
    protected volatile boolean stopped = false;


    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
        this.latch = new CountDownLatch(1);
    }


    public abstract String getServiceName();


    public void start() {
        this.thread.start();
    }


    public void shutdown() {
        this.shutdown(false);
    }


    public void stop() {
        this.stop(false);
    }


    public void makeStop() {
        this.stopped = true;
        LOGGER.info("makestop thread " + this.getServiceName());
    }


    public void stop(final boolean interrupt) {
        this.stopped = true;
        LOGGER.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);
        if (hasNotified.compareAndSet(false, true)) {
            this.latch.countDown();
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }


    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        LOGGER.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        if (hasNotified.compareAndSet(false, true)) {
            this.latch.countDown();
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            LOGGER.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                    + this.getJoinTime());
        } catch (InterruptedException e) {
            LOGGER.error("{} is interrupted", getServiceName(), e);
        }
    }


    public void wakeUp() {
        if (hasNotified.compareAndSet(false, true)) {
            this.latch.countDown();
        }
    }


    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        this.latch.reset();
        try {
            if (interval > 0) {
                this.latch.await(interval, TimeUnit.MILLISECONDS);
            } else {
                this.latch.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("{} is interrupted", getServiceName(), e);
        } finally {
            this.hasNotified.set(false);
            this.latch.reset();
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }


    public boolean isStopped() {
        return stopped;
    }


    public long getJoinTime() {
        return JoinTime;
    }
}

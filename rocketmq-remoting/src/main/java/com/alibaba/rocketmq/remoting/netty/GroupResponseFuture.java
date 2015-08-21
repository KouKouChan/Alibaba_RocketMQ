package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;

import java.util.concurrent.Semaphore;

public class GroupResponseFuture extends ResponseFuture {

    private final Semaphore semaphore;

    public GroupResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback,
                               SemaphoreReleaseOnlyOnce once, int groupSize) {
        super(opaque, timeoutMillis, invokeCallback, once);
        if (groupSize > 0) {
            semaphore = new Semaphore(groupSize);
        } else {
            semaphore = null;
        }
    }

    public boolean countDown() throws InterruptedException {
        if (null != semaphore) {
            semaphore.acquire();
            return semaphore.availablePermits() <= 0;
        }
        return true;
    }
}

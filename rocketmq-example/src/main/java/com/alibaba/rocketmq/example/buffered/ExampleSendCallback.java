package com.alibaba.rocketmq.example.buffered;

import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * <strong>Warning:</strong>If the message is not sent successfully for the first time, the statistics won't be correct.
 * </p>
 * @author Li Zhanhui
 */
public class ExampleSendCallback implements SendCallback {

    @Override
    public void onSuccess(SendResult sendResult) {
        System.out.println(sendResult);
    }

    @Override
    public void onException(Throwable e) {
    }
}

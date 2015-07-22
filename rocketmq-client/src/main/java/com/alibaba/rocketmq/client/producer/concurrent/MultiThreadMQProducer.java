package com.alibaba.rocketmq.client.producer.concurrent;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.TraceLevel;
import com.alibaba.rocketmq.client.producer.selector.SelectMessageQueueByDataCenter;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadMQProducer {

    private static final Logger LOGGER = ClientLogger.getLog();

    private static final int TPS_TOL = 100;

    private static final int MAX_NUMBER_OF_MESSAGE_IN_QUEUE = 50000;

    private final ScheduledExecutorService resendFailureMessagePoolExecutor;

    private static final int NUMBER_OF_EMBEDDED_PRODUCERS = 4;

    private final List<DefaultMQProducer> defaultMQProducers = new ArrayList<DefaultMQProducer>();

    private SendCallback sendCallback;

    private LocalMessageStore localMessageStore;

    private volatile boolean started;

    private final CustomizableSemaphore semaphore;

    private int semaphoreCapacity;

    private final AtomicLong successSendingCounter = new AtomicLong(0L);

    private long lastSuccessfulSendingCount = 0L;

    private long lastStatsTimeStamp = System.currentTimeMillis();

    private float officialTps = 0.0F;

    private float accumulativeTPSDelta = 0.0F;

    private int count;

    private final List<MessageQueueSelector> messageQueueSelectors = new ArrayList<MessageQueueSelector>();

    private LinkedBlockingQueue<Message> messageQueue;

    private MessageSender messageSender;

    private volatile long waitResponseTimeoutCounter = 0;

    public MultiThreadMQProducer(MultiThreadMQProducerConfiguration configuration) {

        try {
            if (null == configuration) {
                throw new IllegalArgumentException("MultiThreadMQProducerConfiguration cannot be null");
            }

            resendFailureMessagePoolExecutor = Executors
                    .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ResendFailureMessageService"));

            semaphoreCapacity = configuration.getInitialNumberOfMessagePermits();

            semaphore = new CustomizableSemaphore(semaphoreCapacity, true);

            for (int i = 0; i < NUMBER_OF_EMBEDDED_PRODUCERS; i++) {
                DefaultMQProducer defaultMQProducer = new DefaultMQProducer(configuration.getProducerGroup() + "_" + (i + 1));

                //Configure default producer.
                defaultMQProducer.setDefaultTopicQueueNums(configuration.getDefaultTopicQueueNumber());
                defaultMQProducer.setRetryTimesWhenSendFailed(configuration.getRetryTimesBeforeSendingFailureClaimed());
                defaultMQProducer.setSendMsgTimeout(configuration.getSendMessageTimeOutInMilliSeconds());
                defaultMQProducer.setTraceLevel(TraceLevel.PRODUCTION.name());
                defaultMQProducers.add(defaultMQProducer);
            }

            for (DefaultMQProducer defaultMQProducer : defaultMQProducers) {
                defaultMQProducer.start();
            }

            localMessageStore = new DefaultLocalMessageStore(configuration.getProducerGroup());

            startResendFailureMessageService(configuration.getResendFailureMessageDelay());

            startMonitorTPS();

            for (DefaultMQProducer defaultMQProducer : defaultMQProducers) {
                messageQueueSelectors.add(new SelectMessageQueueByDataCenter(defaultMQProducer));
            }

            messageQueue = new LinkedBlockingQueue<Message>(MAX_NUMBER_OF_MESSAGE_IN_QUEUE);

            addShutdownHook();

            messageSender = new MessageSender();

            Thread messageSendingThread = new Thread(messageSender);
            messageSendingThread.setName("MessageSendingService");
            messageSendingThread.start();
            started = true;
        } catch (Exception e) {
            LOGGER.error("Fatal error while instantiating MultiThreadMQProducer", e);
            throw new RuntimeException("Initialization error", e);
        } finally {
            if (started) {
                LOGGER.debug("Client starts successfully");
            } else {
                LOGGER.error("Client starts with error.");
            }
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.warn("Multi-thread MQ Producer shutdown hook invoked.");
                try {
                    shutdown();
                } catch (InterruptedException e) {
                    LOGGER.error("ShutdownHook error.", e);
                }
                LOGGER.warn("Multi-thread MQ Producer shutdown completes.");
            }
        });
    }

    private void startMonitorTPS() {
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TPSMonitorService"))
                .scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            LOGGER.debug("Monitoring TPS and adjusting semaphore capacity service starts.");
                            float tps = (successSendingCounter.longValue() - lastSuccessfulSendingCount) * 1000.0F
                                    / (System.currentTimeMillis() - lastStatsTimeStamp);

                            LOGGER.debug("Current TPS: " + tps +
                                    "; Number of message pending to send is: " + messageQueue.size() +
                                    "; Number of message stashed to local message store is: " + localMessageStore.getNumberOfMessageStashed() +
                                    "; Number of message already sent is: " + successSendingCounter.longValue());

                            count++;

                            if (tps > officialTps + TPS_TOL || tps < officialTps - TPS_TOL) {
                                adjustThrottle(tps);
                            } else {
                                accumulativeTPSDelta += tps - officialTps;
                                if (Math.abs(accumulativeTPSDelta) > TPS_TOL) {
                                    adjustThrottle(tps);
                                }
                            }

                            lastStatsTimeStamp = System.currentTimeMillis();
                            lastSuccessfulSendingCount = successSendingCounter.longValue();

                        } catch (Exception e) {
                            LOGGER.error("Monitor TPS error", e);
                        } finally {
                            LOGGER.debug("Monitoring TPS and adjusting semaphore capacity service completes.");
                        }
                    }

                    private void adjustThrottle(float tps) {
                        LOGGER.debug("Begin to adjust throttle. Current semaphore capacity is: " + semaphoreCapacity);
                        int updatedSemaphoreCapacity = 0;
                        if (tps > officialTps) {
                            if (accumulativeTPSDelta > TPS_TOL) { //Update due to accumulative TPS delta surpass TPS_TOL
                                updatedSemaphoreCapacity = Math.min(semaphoreCapacity + (int) accumulativeTPSDelta / count,
                                        MultiThreadMQProducerConfiguration.MAXIMUM_NUMBER_OF_MESSAGE_PERMITS);
                            } else { //Update due to a specific second-average TPS > officialTPS + TPS_TOL
                                updatedSemaphoreCapacity = Math.min(semaphoreCapacity + (int) (tps - officialTps) + 1,
                                        MultiThreadMQProducerConfiguration.MAXIMUM_NUMBER_OF_MESSAGE_PERMITS);
                            }

                            if (updatedSemaphoreCapacity > semaphoreCapacity) {
                                semaphore.release(updatedSemaphoreCapacity - semaphoreCapacity);
                                semaphoreCapacity = updatedSemaphoreCapacity;
                            }
                        } else {
                            if (-1 * accumulativeTPSDelta > TPS_TOL) { //Update due to accumulative TPS delta surpass TPS_TOL
                                updatedSemaphoreCapacity = Math.max(semaphoreCapacity + (int) accumulativeTPSDelta / count,
                                        MultiThreadMQProducerConfiguration.MINIMUM_NUMBER_OF_MESSAGE_PERMITS);
                            } else { //Update due to a specific second-average TPS < officialTPS - TPS_TOL
                                updatedSemaphoreCapacity = Math.max(semaphoreCapacity + (int) (tps - officialTps) - 1,
                                        MultiThreadMQProducerConfiguration.MINIMUM_NUMBER_OF_MESSAGE_PERMITS);
                            }

                            if (updatedSemaphoreCapacity < semaphoreCapacity) {
                                int delta = semaphoreCapacity - updatedSemaphoreCapacity;
                                semaphore.reducePermits(delta);
                                semaphoreCapacity = updatedSemaphoreCapacity;
                            }
                        }

                        //Update official TPS.
                        officialTps = tps;

                        //reset accumulative TPS delta.
                        accumulativeTPSDelta = 0.0F;

                        //reset count.
                        count = 0;

                        LOGGER.debug("Semaphore capacity adjusted to:" + semaphoreCapacity);
                    }

                }, 3000, 1000, TimeUnit.MILLISECONDS);

    }

    public void startResendFailureMessageService(long interval) {
        resendFailureMessagePoolExecutor.scheduleWithFixedDelay(
                new ResendMessageTask(localMessageStore, this), 3100, interval, TimeUnit.MILLISECONDS);
        LOGGER.info("Resend failure message service starts.");
    }

    public void registerCallback(SendCallback sendCallback) {
        this.sendCallback = sendCallback;
    }

    public void handleSendMessageFailure(Message msg, Throwable e) {
        //Release assigned token.
        semaphore.release();

        localMessageStore.stash(msg);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.error("#handleSendMessageFailure: Send message failed. Enter re-send logic. Exception:", e);
        } else if (e instanceof MQClientException && e.getMessage().contains("timeout")) {
            waitResponseTimeoutCounter++;
            if (waitResponseTimeoutCounter % 1000 == 0) {
                LOGGER.error("#handleSendMessageFailure: Send message failed. Enter re-send logic. Exception:", e);
            }
        } else {
            LOGGER.error("#handleSendMessageFailure: Send message failed. Enter re-send logic. Exception: ", e);
        }
    }

    public void send(final Message msg) {
        send(msg, false);
    }

    public void send(final Message[] messages) {
        send(messages, false);
    }

    protected void send(Message message, boolean hasToken) {

        if (null == message) {
            if (hasToken) {
                semaphore.release();
            }

            return;
        }

        if (hasToken) {
            try {
                if (messageQueue.remainingCapacity() > 0) {
                    if (!messageQueue.offer(message, 50, TimeUnit.MILLISECONDS)) {
                        semaphore.release();
                        localMessageStore.stash(message);
                    }
                } else {
                    semaphore.release();
                    localMessageStore.stash(message);
                }
            } catch (InterruptedException e) {
                handleSendMessageFailure(message, e);
            }
        } else {
            if (semaphore.tryAcquire()) {
                try {
                    if (messageQueue.remainingCapacity() > 0) {
                        if (!messageQueue.offer(message, 50, TimeUnit.MILLISECONDS)) {
                            semaphore.release();
                            localMessageStore.stash(message);
                        }
                    } else {
                        semaphore.release();
                        localMessageStore.stash(message);
                    }
                } catch (InterruptedException e) {
                    handleSendMessageFailure(message, e);
                }
            } else {
                localMessageStore.stash(message);
            }
        }
    }

    /**
     * This method would send message with or without token from semaphore. Ultimate client user is not supposed to use
     * this method unless you know what you are doing.
     *
     * @param messages  Messages to send.
     * @param hasTokens If these messages have already been assigned with tokens: true for yes; false for no.
     */
    protected void send(final Message[] messages, boolean hasTokens) {
        if (null == messages || messages.length == 0) {
            return;
        }

        for (Message message : messages) {
            send(message, hasTokens);
        }
    }

    public static MultiThreadMQProducerConfiguration configure() {
        return new MultiThreadMQProducerConfiguration();
    }

    /**
     * This method properly shutdown this producer client.
     *
     * @throws InterruptedException if unable to shutdown within 1 minute.
     */
    public void shutdown() throws InterruptedException {
        LOGGER.warn("MultiThreadMQProducer starts to shutdown.");
        //No more messages from client or local message store.
        semaphore.drainPermits();

        //Stop thread which pops messages from local message store.
        resendFailureMessagePoolExecutor.shutdown();
        resendFailureMessagePoolExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);

        messageSender.stop();

        //Stop defaultMQProducer.
        for (DefaultMQProducer defaultMQProducer : defaultMQProducers) {
            defaultMQProducer.shutdown();
        }

        Message message = null;
        while (messageQueue.size() > 0) {
            message = messageQueue.poll();
            if (null != message) {
                localMessageStore.stash(message);
            }
        }

        //Refresh local message store configuration file.
        if (null != localMessageStore) {
            localMessageStore.close();
            localMessageStore = null;
        }

        LOGGER.warn("MultiThreadMQProducer shuts down completely.");
    }

    public CustomizableSemaphore getSemaphore() {
        return semaphore;
    }

    public AtomicLong getSuccessSendingCounter() {
        return successSendingCounter;
    }

    /**
     * This class is to expose reducePermits(int reduction) method publicly.
     */
    public static class CustomizableSemaphore extends Semaphore {
        public CustomizableSemaphore(int permits) {
            super(permits);
        }

        public CustomizableSemaphore(int permits, boolean fair) {
            super(permits, fair);
        }

        /**
         * Override to expose this method publicly.
         *
         * @param reduction amount of permits to reduce.
         */
        @Override
        public void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }


    class MessageSender implements Runnable {
        private boolean running = true;
        private long roundRobinCounter = 0;
        @Override
        public void run() {
            while (running) {
                Message message = null;
                try {
                    message = messageQueue.take();
                    int loopRemain = (int)((roundRobinCounter++) % NUMBER_OF_EMBEDDED_PRODUCERS);
                    defaultMQProducers.get(loopRemain).send(message, messageQueueSelectors.get(loopRemain), null,
                            new SendMessageCallback(MultiThreadMQProducer.this, sendCallback, message));
                } catch (Exception e) {
                    if (null != message) {
                        handleSendMessageFailure(message, e);
                    } else {
                        LOGGER.error("Message being null when exception raised.", e);
                    }
                }
            }
        }

        public void stop() {
            running = false;
        }
    }
}
package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageEncoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultLocalMessageStoreTest {

    @BeforeClass
    public static void setEnv() {
        System.setProperty("log.home", System.getProperty("user.home"));
    }

    private static final String STORE_NAME = "test_local_message_store";

    private DefaultLocalMessageStore defaultLocalMessageStore;

    private void cleanUp() {
        File storeFile = StoreHelper.getLocalMessageStoreDirectory(STORE_NAME);
        if (storeFile.exists()) {
            recursiveDelete(storeFile);
        }
    }

    private static void recursiveDelete(File file) {
        if (!file.isDirectory()) {
            file.delete();
        }

        File[] files = file.listFiles();
        if (null != files && files.length > 0) {
            for (File f : files) {
                recursiveDelete(f);
            }
        }

        file.delete();
    }

    @Before
    public void setUp() throws IOException {
        cleanUp();
        defaultLocalMessageStore = new DefaultLocalMessageStore(STORE_NAME);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (null != defaultLocalMessageStore) {
            defaultLocalMessageStore.close();
        }
        cleanUp();
    }

    @Test
    public void testStashAndPop() throws InterruptedException, IOException {
        int totalMessageCount = 1000;
        String topic = "Topic";
        byte[] body = "Data".getBytes();
        String key = "abc";
        String value = "value";

        for (int i = 0; i < totalMessageCount; i++) {
            Message message = new Message(topic, body);
            message.setKey(key);
            message.putUserProperty(key, value);
            defaultLocalMessageStore.stash(message);
        }
        defaultLocalMessageStore.close();

        defaultLocalMessageStore = new DefaultLocalMessageStore(STORE_NAME);
        int poppedOut = 0;
        Message[] messages = defaultLocalMessageStore.pop(20);
        while (null != messages && messages.length > 0) {
            poppedOut += messages.length;
            for (Message msg : messages) {
                Assert.assertEquals(topic, msg.getTopic());
                Assert.assertArrayEquals(body, msg.getBody());
                Assert.assertEquals(key, msg.getKeys());
                Assert.assertEquals(value, msg.getProperty(key));
            }
            messages = defaultLocalMessageStore.pop(2);
        }
        Assert.assertEquals(totalMessageCount, poppedOut);
        defaultLocalMessageStore.close();
    }


    @Test
    public void testDataRecovery() throws InterruptedException, IOException {
        int totalMessageCount = 1000;
        String topic = "Topic";
        byte[] body = "Data".getBytes();
        String key = "abc";
        String value = "value";

        for (int i = 0; i < totalMessageCount; i++) {
            Message message = new Message(topic, body);
            message.setKey(key);
            message.putUserProperty(key, value);
            defaultLocalMessageStore.stash(message);
        }
        defaultLocalMessageStore.close();

        // Delete .config file
        File storeFile = StoreHelper.getLocalMessageStoreDirectory(STORE_NAME);
        File configFile = new File(storeFile, ".config");
        if (configFile.exists()) {
            configFile.delete();
        } else {
            Assert.fail(".config file does not exist");
        }

        // Start recovery.
        defaultLocalMessageStore = new DefaultLocalMessageStore(STORE_NAME);
        int poppedOut = 0;
        Message[] messages = defaultLocalMessageStore.pop(20);
        while (null != messages && messages.length > 0) {
            poppedOut += messages.length;
            for (Message msg : messages) {
                Assert.assertEquals(topic, msg.getTopic());
                Assert.assertArrayEquals(body, msg.getBody());
                Assert.assertEquals(key, msg.getKeys());
                Assert.assertEquals(value, msg.getProperty(key));
            }
            messages = defaultLocalMessageStore.pop(2);
        }
        Assert.assertEquals(totalMessageCount, poppedOut);
        defaultLocalMessageStore.close();
    }

    @Test
    public void testStashAndPopMulti() throws InterruptedException, IOException {
        final int totalMessageCount = Integer.parseInt(System.getProperty("messagecounts", "10000"));
        final int poolSize = Integer.parseInt(System.getProperty("poolsize", "10"));
        final String topic = "Topic";
        final byte[] body = "Data".getBytes();
        final String key = "abc";
        final String value = "value";
        final AtomicInteger counter = new AtomicInteger(0);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(poolSize);
        final CountDownLatch countDownLatch = new CountDownLatch(poolSize);
        for (int i = 0; i < poolSize; i++) {
            scheduledExecutorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < totalMessageCount; i++) {
                        Message message = new Message(topic, body);
                        message.setKey(key);
                        message.putUserProperty(key, value);
                        defaultLocalMessageStore.stash(message);
                        counter.incrementAndGet();
                    }
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        defaultLocalMessageStore.close();


        defaultLocalMessageStore = new DefaultLocalMessageStore(STORE_NAME);
        Message[] messages = defaultLocalMessageStore.pop(20);
        while (null != messages && messages.length > 0) {
            for (Message msg : messages) {
                Assert.assertEquals(topic, msg.getTopic());
                Assert.assertArrayEquals(body, msg.getBody());
                Assert.assertEquals(key, msg.getKeys());
                Assert.assertEquals(value, msg.getProperty(key));
                counter.decrementAndGet();
            }
            messages = defaultLocalMessageStore.pop(2);
        }
        defaultLocalMessageStore.close();

        Assert.assertEquals(0, counter.intValue());
    }

    @Test
    public void testStashAndPopHugeMulti() throws InterruptedException, IOException {
        final int totalMessageCount = Integer.parseInt(System.getProperty("messagecounts", "1000"));
        final int poolSize = Integer.parseInt(System.getProperty("poolsize", "100"));
        final String topic = "Topic";
        final byte[] body = "Data".getBytes();
        final String key = "abc";
        final String value = "value";
        final AtomicInteger counter = new AtomicInteger(0);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(poolSize);
        final CountDownLatch countDownLatch = new CountDownLatch(poolSize);

        for (int i = 0; i < poolSize; i++) {
            scheduledExecutorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < totalMessageCount; i++) {
                        Message message = new Message(topic, body);
                        message.setKey(key);
                        message.putUserProperty(key, value);
                        defaultLocalMessageStore.stash(message);
                        counter.incrementAndGet();
                    }
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        defaultLocalMessageStore.close();


        defaultLocalMessageStore = new DefaultLocalMessageStore(STORE_NAME);
        Message[] messages = defaultLocalMessageStore.pop(20);
        while (null != messages && messages.length > 0) {
            for (Message msg : messages) {
                Assert.assertEquals(topic, msg.getTopic());
                Assert.assertArrayEquals(body, msg.getBody());
                Assert.assertEquals(key, msg.getKeys());
                Assert.assertEquals(value, msg.getProperty(key));
                counter.decrementAndGet();
            }
            messages = defaultLocalMessageStore.pop(2);
        }
        defaultLocalMessageStore.close();

        Assert.assertEquals(0, counter.intValue());
    }


    @Test
    public void testEncode() {

        Message message = new Message("中文", "abc中文abc".getBytes(Charset.forName("UTF-8")));
        ByteBuffer data = MessageEncoder.encode(StoreHelper.wrap(message));
        Message msg = MessageDecoder.decode(data, true, false);

        if (null == msg) {
            Assert.fail("Failed to decode msg");
        }

        Assert.assertEquals(message.getTopic(), msg.getTopic());
        Assert.assertArrayEquals(message.getBody(), msg.getBody());
    }
}
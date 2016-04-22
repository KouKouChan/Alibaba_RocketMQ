package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class MappedFileLocalMessageStoreTest {

    private MappedFileLocalMessageStore store;

    private static String storePath = "test";

    @Before
    public void setUp() throws Exception {
        store = new MappedFileLocalMessageStore(storePath);
        store.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        store.close();

        File storeDirectory = StoreHelper.getLocalMessageStoreDirectory(storePath);
        StoreHelper.delete(storeDirectory, true);
    }

    @Test
    public void testPop() throws IOException {
        Message[] messages = store.pop(1000);
        Assert.assertEquals(0, messages.length);
    }

    @Test
    public void testStash() throws Exception {
        Message message = new Message("Test", "Test123".getBytes());
        store.stash(message);

        // Assert.assertEquals(1, store.getNumberOfMessageStashed());

        Message[] messages = store.pop(10);
        for (Message msg : messages) {
            System.out.println(msg);
            System.out.println(new String(msg.getBody(), "UTF-8"));
        }
    }


    @Test
    public void benchmark() {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        Message message = new Message("TestTopic", data);
        int max = 10000;
        for (int i = 0; i < max; i++) {
            store.stash(message);
        }

        Assert.assertEquals(max, store.getNumberOfMessageStashed());

        int count = 0;
        for (int i = 0; i < max; i++) {
            Message[] msgs = store.pop(10);
            count += msgs.length;

            if (msgs.length == 0) {
                break;
            }

            for (Message msg : msgs) {
                Assert.assertEquals("TestTopic", msg.getTopic());
                Assert.assertArrayEquals(data, msg.getBody());
            }
        }

        Assert.assertEquals(max, count);
    }

    @Test
    public void testMultiThread() throws Exception {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        Message message = new Message("TestTopic", data);

        final int threshold = 100000;
        int threadCount = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        final Semaphore semaphore = new Semaphore(threshold);
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(new TaskRunner(store, semaphore, message, countDownLatch));
        }

        countDownLatch.await();
        Assert.assertEquals(threshold, store.getNumberOfMessageStashed());
        executorService.shutdown();
    }

    static class TaskRunner implements Runnable {

        private final Semaphore semaphore;

        private final Message message;

        private final CountDownLatch countDownLatch;

        private final MappedFileLocalMessageStore store;

        public TaskRunner(final MappedFileLocalMessageStore store,
                          final Semaphore semaphore,
                          final Message message, final CountDownLatch countDownLatch) {
            this.store = store;
            this.semaphore = semaphore;
            this.message = message;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                while (semaphore.tryAcquire()) {
                    if (!store.stash(message)) {
                        semaphore.release();
                    }
                }
                countDownLatch.countDown();
            } catch (Throwable ignored) {

            }
        }
    }

    public static void main(String[] args) throws IOException {
        MappedFileLocalMessageStore store = new MappedFileLocalMessageStore("test");
        store.start();

        Message[] messages = store.pop(100);
        for (Message message : messages) {
            System.out.println(message);
        }

        Message message = new Message("topic", "body".getBytes());
        store.stash(message);

        //store.close();
    }
}
package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.store.AllocateMappedFileService;
import org.junit.*;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MappedFileLocalMessageStoreTest {

    private MappedFileLocalMessageStore store;

    private static String storePath;

    @BeforeClass
    public static void init() {
        String home = System.getProperty("user.home");
        storePath = home + "/local_store";
    }

    @Before
    public void setUp() throws Exception {
        store = new MappedFileLocalMessageStore(storePath);
        store.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        store.close();

        File storeDirectory = new File(storePath);
        File[] files = storeDirectory.listFiles();
        for (File file : files) {
            file.delete();
        }
    }

    @Test
    public void testStash() throws Exception {
        Message message = new Message("Test", "Test123".getBytes());
        store.stash(message);

        Assert.assertEquals(1, store.getNumberOfMessageStashed());

        Message[] messages = store.pop(10);
        for (Message msg : messages) {
            System.out.println(msg);
            System.out.println(new String(msg.getBody(), "UTF-8"));
        }

        store.close();
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
        store.close();
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

    @Test
    public void testMappedFileAllocationService() {
        AllocateMappedFileService service = new AllocateMappedFileService();
        service.start();

        service.stop();
    }
}
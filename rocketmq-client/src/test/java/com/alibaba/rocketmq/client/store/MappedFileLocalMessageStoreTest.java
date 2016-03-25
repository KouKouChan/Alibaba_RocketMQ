package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.store.AllocateMappedFileService;
import org.junit.*;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
    }

    @Test
    public void testStash() throws Exception {
        Message message = new Message("Test", "Test123".getBytes());
        store.stash(message);
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

        for (int i = 0; i < 10000; i++) {
            store.stash(message);
        }

        int count = 0;
        for (int i = 0; i < 1001; i++) {
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

        Assert.assertEquals(10000, count);
    }

    // @Test
    public static void main(String[] args) throws Exception {
        init();
        MappedFileLocalMessageStore store = new MappedFileLocalMessageStore(storePath);
        store.start();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'x');
        Message message = new Message("TestTopic", data);

        final AtomicInteger count = new AtomicInteger(0);
        final int threshold = 100000;
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        CountDownLatch countDownLatch = new CountDownLatch(4);

        executorService.submit(new TaskRunner(store, threshold, count, message, countDownLatch));
        executorService.submit(new TaskRunner(store, threshold, count, message, countDownLatch));
        executorService.submit(new TaskRunner(store, threshold, count, message, countDownLatch));
        executorService.submit(new TaskRunner(store, threshold, count, message, countDownLatch));

        countDownLatch.await();
        executorService.shutdown();
        store.close();
    }

    static class TaskRunner implements Runnable {

        private final int threshold;

        private final AtomicInteger count;

        private final Message message;

        private final CountDownLatch countDownLatch;

        private final MappedFileLocalMessageStore store;

        public TaskRunner(final MappedFileLocalMessageStore store,
                          final int threshold, final AtomicInteger count,
                          final Message message, final CountDownLatch countDownLatch) {
            this.store = store;
            this.threshold = threshold;
            this.count = count;
            this.message = message;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                while (count.get() < threshold) {
                    if (store.stash(message)) {
                       count.incrementAndGet();
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
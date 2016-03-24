package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import org.junit.*;

import java.util.Arrays;

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
}
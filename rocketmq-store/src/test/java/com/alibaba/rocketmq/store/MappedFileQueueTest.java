/**
 * $Id: MappedFileQueueTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import org.junit.*;

import static org.junit.Assert.*;


public class MappedFileQueueTest {

    // private static final String StoreMessage =
    // "Once, there was a chance for me! but I did not treasure it. if";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }


    @Before
    public void setUp() throws Exception {
    }


    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void test_getLastMappedFile() {
        final String fixedMsg = "0123456789abcdef";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/a/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            if (!result) {
                System.out.println("appendMessage " + i);
            }
            assertTrue(result);
        }

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.getLastMappedFile() OK");
    }


    @Test
    public void test_findMappedFileByOffset() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/b/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            // System.out.println("appendMessage " + bytes);
            assertTrue(result);
        }

        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(0);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4);
        assertTrue(mappedFile == null);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4 + 100);
        assertTrue(mappedFile == null);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.findMappedFileByOffset() OK");
    }


    @Test
    public void test_commit() {
        final String fixedMsg = "0123456789abcdef";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/c/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        // 不断尝试提交
        boolean result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 1, mappedFileQueue.getCommittedWhere());
        System.out.println("1 " + result + " " + mappedFileQueue.getCommittedWhere());

        result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 2, mappedFileQueue.getCommittedWhere());
        System.out.println("2 " + result + " " + mappedFileQueue.getCommittedWhere());

        result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 3, mappedFileQueue.getCommittedWhere());
        System.out.println("3 " + result + " " + mappedFileQueue.getCommittedWhere());

        result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 4, mappedFileQueue.getCommittedWhere());
        System.out.println("4 " + result + " " + mappedFileQueue.getCommittedWhere());

        result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 5, mappedFileQueue.getCommittedWhere());
        System.out.println("5 " + result + " " + mappedFileQueue.getCommittedWhere());

        result = mappedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 6, mappedFileQueue.getCommittedWhere());
        System.out.println("6 " + result + " " + mappedFileQueue.getCommittedWhere());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.commit() OK");
    }


    @Test
    public void test_getMappedMemorySize() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/d/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        assertEquals(fixedMsg.length() * 1024, mappedFileQueue.getMappedMemorySize());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.getMappedMemorySize() OK");
    }

}

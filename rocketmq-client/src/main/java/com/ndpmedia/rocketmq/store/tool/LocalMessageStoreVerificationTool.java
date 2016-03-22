/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.ndpmedia.rocketmq.store.tool;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalMessageStoreVerificationTool {

    private static final String STORE_FILE_NAME_REGEX = "\\d+";

    private static final Pattern STORE_FILE_NAME_PATTERN = Pattern.compile(STORE_FILE_NAME_REGEX);

    private static final int MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;

    private static final String CONFIG_FILE_NAME = ".config";

    private static DefaultMQProducer producer;


    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            System.out.println("Usage: java -cp rocketmq-client-3.2.2.jar com.ndpmedia.rocketmq.store.tool.LocalMessageStoreVerificationTool /path/to/store");
            return;
        }

        try {
            System.setProperty("enable_ssl", "true");
            producer = new DefaultMQProducer("Tool");
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            System.exit(1);
        }

        checkRecursively(new File(args[0]));

        producer.shutdown();
    }

    private static void checkRecursively(File file) throws IOException {
        if (file.isFile()) {
            checkFile(file);
        } else {
            String[] files = file.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    File f = new File(dir, name);
                    if (f.isDirectory()) {
                        return true;
                    }

                    Matcher matcher = STORE_FILE_NAME_PATTERN.matcher(name);
                    return matcher.matches();
                }
            });

            for (String f : files) {
                checkRecursively(new File(file, f));
            }
        }
    }

    private static void checkFile(File file) throws IOException {

        File log = new File(new File(System.getProperty("user.home")), "fail.log");
        if (!log.getParentFile().exists()) {
            if (!log.getParentFile().mkdirs()) {
                System.exit(1);
            }
        }

        if (!log.exists()) {
            if (!log.createNewFile()) {
                System.exit(1);
            }
        }

        File configFile = new File(file.getParentFile(), CONFIG_FILE_NAME);
        InputStream inputStream = null;
        AtomicLong writeIndex = new AtomicLong();
        AtomicLong writeOffSet = new AtomicLong();
        AtomicLong readIndex = new AtomicLong();
        AtomicLong readOffSet = new AtomicLong();

        try {
            inputStream = new FileInputStream(configFile);
            Properties properties = new Properties();
            properties.load(inputStream);

            writeIndex.set(null == properties.getProperty("writeIndex") ? 0L :
                    Long.parseLong(properties.getProperty("writeIndex")));
            writeOffSet.set(null == properties.getProperty("writeOffSet") ? 0L :
                    Long.parseLong(properties.getProperty("writeOffSet")));
            readIndex.set(null == properties.getProperty("readIndex") ? 0L :
                    Long.parseLong(properties.getProperty("readIndex")));
            readOffSet.set(null == properties.getProperty("readOffSet") ? 0L :
                    Long.parseLong(properties.getProperty("readOffSet")));
        } catch (IOException ignored) {
        } catch (NumberFormatException ignored) {
        } finally {
            if (null != inputStream) {
                inputStream.close();
            }
        }

        BufferedWriter bos = new BufferedWriter(new FileWriter(log, true));
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        boolean hasError = false;
        int count = 0;
        int fileNameNum = Integer.parseInt(file.getName());
        while (randomAccessFile.getFilePointer() + 4 + 4 < randomAccessFile.length()) {
            int msgSize = randomAccessFile.readInt();
            int magicCode = randomAccessFile.readInt();

            if (magicCode != MAGIC_CODE) {
                System.err.println("Illegal magic code found! Position: " + (randomAccessFile.getFilePointer() - 4));
                System.err.println("Illegal Code: [" + magicCode + "], Assumed Code: [" + MAGIC_CODE + "]");
                hasError = true;
                // break;
            }
            if (!hasError) {
                byte[] data = new byte[msgSize - 4 - 4];
                randomAccessFile.readFully(data);
                ByteBuffer byteBuffer = ByteBuffer.allocate(msgSize);
                byteBuffer.putInt(msgSize);
                byteBuffer.putInt(magicCode);
                byteBuffer.put(data);
                byteBuffer.flip();
                Message message = MessageDecoder.decode(byteBuffer, true, true);
                System.out.println("Msg Count: " + (++count));

                if (fileNameNum > readIndex.get() || count > readIndex.get()) {
                    try {
                        producer.send(message);
                        System.out.println(message.getTopic());
                        System.out.println("Msg Size: " + msgSize);
                        System.out.println("Msg Body: " + new String(message.getBody(), "UTF-8"));
                    } catch (Exception e) {
                        bos.write(String.valueOf(count));
                        bos.newLine();
                        bos.flush();
                    }
                }
            } else {
                while (randomAccessFile.readInt() != MAGIC_CODE) {

                    if (randomAccessFile.getFilePointer() >= randomAccessFile.length()) {
                        break;
                    }

                    // keep reading
                }
                randomAccessFile.seek(randomAccessFile.getFilePointer() - 8);
                hasError = false;
            }
        }

        randomAccessFile.close();
        bos.close();
    }

}

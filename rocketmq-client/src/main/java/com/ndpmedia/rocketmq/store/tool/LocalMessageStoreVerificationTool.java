/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.ndpmedia.rocketmq.store.tool;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalMessageStoreVerificationTool {

    private static final String STORE_FILE_NAME_REGEX = "\\d+";

    private static final Pattern STORE_FILE_NAME_PATTERN = Pattern.compile(STORE_FILE_NAME_REGEX);

    private static final int MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;

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

        BufferedWriter bos = new BufferedWriter(new FileWriter(log, true));
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        boolean hasError = false;
        int count = 0;
        while (randomAccessFile.getFilePointer() + 4 + 4 < randomAccessFile.length()) {
            int msgSize = randomAccessFile.readInt();
            int magicCode = randomAccessFile.readInt();

            if (magicCode != MAGIC_CODE) {
                System.err.println("Illegal magic code found! Position: " + (randomAccessFile.getFilePointer() - 4));
                System.err.println("Illegal Code: [" + magicCode + "], Assumed Code: [" + MAGIC_CODE + "]");
                hasError = true;
                // break;
            }
            long pos = 0;
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
                if (count > 2012) {
                    if (message.getTopic().equals("T_YMREDIRECTOR_QUEUE_DRUID")) {
                        try {
                            producer.send(message);
                        } catch (Exception e) {
                            bos.write(String.valueOf(count));
                            bos.newLine();
                            bos.flush();
                        }
                        System.out.println(message.getTopic());
                        System.out.println("Msg Size: " + msgSize);
                        System.out.println("Msg Body: " + new String(message.getBody(), "UTF-8"));
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

        if (hasError) {
            System.err.println("Fatal: File " + file.getAbsolutePath() + " is tampered!");
        }

    }

}

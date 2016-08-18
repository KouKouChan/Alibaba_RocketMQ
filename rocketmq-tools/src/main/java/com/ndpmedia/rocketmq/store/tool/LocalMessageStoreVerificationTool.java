/*
 * Copyright (c) 2015. All Rights Reserved.
 */
package com.ndpmedia.rocketmq.store.tool;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.selector.SelectMessageQueueByDataCenter;
import com.alibaba.rocketmq.client.store.DefaultLocalMessageStore;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalMessageStoreVerificationTool {

    private static final String STORE_FILE_NAME_REGEX = "\\d+";

    private static final Pattern STORE_FILE_NAME_PATTERN = Pattern.compile(STORE_FILE_NAME_REGEX);

    private static final int MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;

    private static final String CONFIG_FILE_NAME = ".config";

    private static DefaultMQProducer producer;
    private static MessageQueueSelector messageQueueSelector = new ExampleMessageQueueSelector();

    private static void showUsageAndExit(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("localMessageStoreVerificationTool", options);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        options.addOption("p", "path", true, "local message store path, required");
        options.addOption("r", "rate", true, "Send message rate, optional");

        CommandLineParser commandLineParser = new DefaultParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);

            if (!commandLine.hasOption("p")) {
                showUsageAndExit(options);
            }

            String path = commandLine.getOptionValue("p");
            float rate = -1F;
            if (commandLine.hasOption("r")) {
                rate = Float.parseFloat(commandLine.getOptionValue("r"));
            }

            try {
                System.setProperty("enable_ssl", "true");
                producer = new DefaultMQProducer("Tool");
                producer.start();
            } catch (MQClientException e) {
                e.printStackTrace();
                System.exit(1);
            }

            checkRecursively(new File(path), rate);

            producer.shutdown();

        } catch (ParseException e) {
            showUsageAndExit(options);
        }


    }

    private static void checkRecursively(File file, float rate) throws IOException {
        if (file.isFile()) {
            checkFile(file, rate);
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

            if (null == files) {
                return;
            }

            for (String f : files) {
                checkRecursively(new File(file, f), rate);
            }
        }
    }

    private static void checkFile(File file, float rate) throws IOException {

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
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        } finally {
            if (null != inputStream) {
                inputStream.close();
            }
        }

        long fileNumber = Long.parseLong(file.getName());
        if (fileNumber + DefaultLocalMessageStore.MESSAGES_PER_FILE < readIndex.get()) {
            return;
        }
        long count = fileNumber;
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        if (readIndex.get() > fileNumber && readIndex.get() < fileNumber + DefaultLocalMessageStore.MESSAGES_PER_FILE) {
            randomAccessFile.seek(readOffSet.get());
            count = readIndex.get();
        }

        boolean hasError = false;

        RateLimiter rateLimiter = null;
        if (rate > 0) {
            rateLimiter = RateLimiter.create(rate);
        }

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
                final Message message = MessageDecoder.decode(byteBuffer, true, true);
                System.out.println("Message recovered");
                System.out.println("Msg Size: " + msgSize);
                try {
                    System.out.println("Begin to send");
                    if (null != rateLimiter) {
                        rateLimiter.acquire();
                    }

                    SendResult sendResult = producer.send(message, messageQueueSelector, null);
                    System.out.println("Sending completes. No." + (++count) + " of " + writeIndex.get());
                    System.out.println("MsgId: " + sendResult.getMsgId());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.err.println("Begin to recover from error magic code.");
                while (randomAccessFile.readInt() != MAGIC_CODE) {
                    if (randomAccessFile.getFilePointer() >= randomAccessFile.length()) {
                        break;
                    }

                    // keep reading
                }
                randomAccessFile.seek(randomAccessFile.getFilePointer() - 8);
                hasError = false;
                System.out.println("Recover from error done. Some broker messages might be skipped.");
            }
        }

        randomAccessFile.close();
    }


    private static class ExampleMessageQueueSelector implements MessageQueueSelector {
        @Override
        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
            List<MessageQueue> messageQueuesList = new ArrayList<>(mqs);
            Collections.shuffle(messageQueuesList);

            for (MessageQueue messageQueue : messageQueuesList) {
                String brokerName = messageQueue.getBrokerName();
                if (SelectMessageQueueByDataCenter.LOCAL_DATA_CENTER_ID.equals(brokerName.split("_")[1])) {
                    return messageQueue;
                }
            }

            return messageQueuesList.get(0);
        }
    }

}
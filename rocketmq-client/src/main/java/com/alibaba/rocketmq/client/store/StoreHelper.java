package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.File;
import java.net.InetSocketAddress;

public final class StoreHelper {

    public static final String DEFAULT_STORE_LOCATION = "/dianyi/data/";

    public static final String LOCAL_MESSAGE_STORE_FOLDER_NAME = ".localMessageStore";


    private StoreHelper() {
    }

    public static MessageExt wrap(Message message) {
        if (message instanceof MessageExt) {
            return (MessageExt)message;
        }

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(message.getTopic());
        messageExt.setFlag(message.getFlag());
        messageExt.setBody(message.getBody());
        MessageAccessor.setProperties(messageExt, message.getProperties());

        messageExt.setBornHost(new InetSocketAddress(0));
        messageExt.setStoreHost(new InetSocketAddress(0));

        return messageExt;
    }

    public static File getLocalMessageStoreDirectory(String storeName) {
        //For convenience of development.
        String storeLocation = System.getProperty("defaultLocalMessageStoreLocation", DEFAULT_STORE_LOCATION);
        if (DEFAULT_STORE_LOCATION.equals(storeLocation)) {
            File defaultStoreLocation = new File(DEFAULT_STORE_LOCATION);
            if (!defaultStoreLocation.exists()) {
                storeLocation = System.getProperty("user.home") + File.separator + LOCAL_MESSAGE_STORE_FOLDER_NAME;
            } else {
                storeLocation = storeLocation.endsWith(File.separator)
                        ? storeLocation + LOCAL_MESSAGE_STORE_FOLDER_NAME
                        : storeLocation + File.separator + LOCAL_MESSAGE_STORE_FOLDER_NAME;
            }
        }
        return new File(storeLocation, storeName);
    }

    public static boolean delete(File file, boolean recursive) {

        if (!file.exists()) {
            return true;
        }

        if (!recursive || file.isFile()) {
            return file.delete();
        } else {
            File[] files = file.listFiles();

            if (null == files) { // Should never happen.
                return true;
            }

            boolean successful = true;
            for (File f : files) {
                successful &= delete(f, true);
            }

            return file.delete() && successful;
        }
    }
}

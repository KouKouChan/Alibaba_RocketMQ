package com.alibaba.rocketmq.client.consumer.rebalance;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Helper {

    public static final String BROKER_NAME_REGEX = "\\p{Alnum}{1,}_(\\d{1,})_\\p{Alnum}{1,}";

    public static final String IP_REGEX = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    public static final Pattern BROKER_NAME_PATTERN = Pattern.compile(BROKER_NAME_REGEX);

    public static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    public static void checkRebalanceParameters(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                                List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }
    }


    public static long ipAddressToNumber(String ipAddress) {
        Matcher matcher = IP_PATTERN.matcher(ipAddress);
        if (matcher.matches()) {
            String[] segments = ipAddress.split("\\.");
            long[] numericalSegments = new long[segments.length];
            for (int i = 0; i < segments.length; i++) {
                numericalSegments[i] = Long.parseLong(segments[i]);
                numericalSegments[i] = numericalSegments[i] << ((segments.length - 1 - i) * 8);
            }
            long result = 0;
            for (long numericalSegment : numericalSegments) {
                result += numericalSegment;
            }
            return result;
        }
        return -1;
    }

}

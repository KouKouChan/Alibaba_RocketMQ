package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;

import java.util.List;
import java.util.Random;

public class Util {

    private static final Logger LOGGER = ClientLogger.getLog();

    private static final String DOCKER_DC_INDEX_ENV_KEY = "ROCKETMQ_DC_INDEX";

    private static final String DOCKER_DC_INDEX_KEY = "DCIndex";

    private static final Random random = new Random(System.currentTimeMillis());

    public static String LOCAL_DATA_CENTER_ID = RemotingUtil.getLocalAddress(false).split("\\.")[1];

    static {
        String dcIndex = System.getenv(DOCKER_DC_INDEX_ENV_KEY);
        if (null == dcIndex) {
            dcIndex = System.getProperty(DOCKER_DC_INDEX_KEY);
        }

        if (null != dcIndex && dcIndex.trim().length() > 0) {
            LOCAL_DATA_CENTER_ID = dcIndex;
        }

        // 10.12 now also in region of US-East
        if ("12".equals(LOCAL_DATA_CENTER_ID)) {
            LOCAL_DATA_CENTER_ID = "1";
        }

        LOGGER.info("DCIndex: {}", LOCAL_DATA_CENTER_ID);
    }

    /**
     * Randomly choose an element from the given list.
     * @param list List of elements, not empty.
     * @return randomly chosen element in the list.
     */
    public static<T> T random(List<T> list) {
        return list.get(random.nextInt(Integer.MAX_VALUE) % list.size());
    }

    public static int getRegionIndex(MessageQueue messageQueue) {
        String brokerName = messageQueue.getBrokerName();
        String[] segments = brokerName.split("_");
        if (segments.length < 3) {
            return -1;
        }

        try {
            return Integer.parseInt(segments[1]);
        } catch (Exception e) {
            return -1;
        }
    }
}

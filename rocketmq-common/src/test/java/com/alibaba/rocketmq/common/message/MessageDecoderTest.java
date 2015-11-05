package com.alibaba.rocketmq.common.message;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MessageDecoderTest {

    private Map<String, String> createMap() {
        Map<String, String> properties = new HashMap<String, String>();
        char[] chars = {1, 2, 3, 4};

        properties.put("abc", new String(chars) + "中文");
        properties.put("中文", "中文");
        properties.put("test", "中文");
        properties.put("英文", "English");

        return properties;
    }

    @Test
    public void testString2messageProperties() throws Exception {
        Map<String, String> properties = createMap();
        String encodedText = MessageDecoder.messageProperties2StringSafe(properties);
        Map<String, String> map = MessageDecoder.string2messagePropertiesSafe(encodedText);
        Assert.assertEquals(properties, map);
    }
}
package com.alibaba.rocketmq.common.message;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MessageDecoderTest {

    @Test
    public void testString2messageProperties() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("中文", "中文");
        properties.put("test", "中文");
        properties.put("英文", "English");

        String encodedText = MessageDecoder.messageProperties2String(properties);

        Map<String, String> map = MessageDecoder.string2messageProperties(encodedText);
        for (Map.Entry<String, String> next : map.entrySet()) {
            Assert.assertTrue(properties.containsKey(next.getKey()));
            Assert.assertEquals(next.getValue(), properties.get(next.getKey()));
        }


        encodedText = MessageDecoder.messageProperties2StringSafe(properties);
        map = MessageDecoder.string2messagePropertiesSafe(encodedText);
        for (Map.Entry<String, String> next : map.entrySet()) {
            Assert.assertTrue(properties.containsKey(next.getKey()));
            Assert.assertEquals(next.getValue(), properties.get(next.getKey()));
        }
    }
}
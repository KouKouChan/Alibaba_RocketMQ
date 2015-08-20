package com.alibaba.rocketmq.example.yeahmobiCR;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.concurrent.MultiThreadMQProducer;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.example.concurrent.ExampleSendCallback;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2015/5/15.
 */
public class StormProducer {

    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0L);

    private static final Random RANDOM = new Random();

    private static final AtomicLong INDEX = new AtomicLong(0L);

    private static final String TOPIC = "T_YMREDIRECTOR_QUEUE_TRACE_EVENT";

    private static final Logger LOGGER = ClientLogger.getLog();

    private static List<String> tId = new ArrayList<String>();
    private static List<String> pId = new ArrayList<String>();
    private static List<String> affId = new ArrayList<String>();
    private static List<String> offId = new ArrayList<String>();
    private static List<String> offId2 = new ArrayList<String>();
    private static List<String> code = new ArrayList<String>();
    private static List<String> status = new ArrayList<String>();

    static{
        tId.add("0");
        tId.add("1");
        tId.add("2");
        tId.add("3");
    }

    static {
        pId.add("0");
        pId.add("1");
        pId.add("2");
        pId.add("3");
        pId.add("4");
    }

    static {
        affId.add("1");
        affId.add("2");
        affId.add("3");
        affId.add("4");
        affId.add("5");
        affId.add("6");
        affId.add("7");
        affId.add("8");
        affId.add("9");
    }

    static {
        offId.add("1");
        offId.add("2");
        offId2.add("3");
        offId2.add("4");
    }

    static {
        code.add("C000");
        code.add("C001");
        code.add("C002");
        code.add("C003");
        code.add("C004");
        code.add("C005");
        code.add("C006");
        code.add("C007");

        code.add("V000");
        code.add("V001");
        code.add("V002");
        code.add("V003");
        code.add("V004");
        code.add("V005");
        code.add("V006");
        code.add("V007");
        code.add("V008");
    }

    static {
        status.add("true");
        status.add("false");
    }

    public static void main(String[] args) {
        int count = 0;
        if (args.length > 0) {
            count = Integer.parseInt(args[0]);
        } else {
            count = -1;
        }
        final AtomicLong successCount = new AtomicLong(0L);
        final AtomicLong lastSent = new AtomicLong(0L);
        final MultiThreadMQProducer producer = MultiThreadMQProducer.configure()
                .configureProducerGroup("PG_YMREDIRECTOR_QUEUE_TRACE_EVENT")//CG_EAGLE_QUEUE_TRACE_EVENT
                .configureRetryTimesBeforeSendingFailureClaimed(3)
                .configureSendMessageTimeOutInMilliSeconds(3000)
                .configureDefaultTopicQueueNumber(16)
                .build();
        producer.registerCallback(new ExampleSendCallback(successCount));

        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TPSService"))
                .scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        long currentSuccessSent = successCount.longValue();
                        LOGGER.info("TPS: " + (currentSuccessSent - lastSent.longValue()) +
                                ". Semaphore available number:" + producer.getSemaphore().availablePermits());
                        lastSent.set(currentSuccessSent);
                    }
                }, 0, 1, TimeUnit.SECONDS);

        if (count < 0) {
            final AtomicLong adder = new AtomicLong(0L);
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MessageManufactureService"))
                    .scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Message[] messages = buildMessages(RANDOM.nextInt(400));
                                producer.send(messages);
                                System.out.println("send OK");
                                adder.incrementAndGet();
                                if (adder.longValue() % 10 == 0) {
                                    LOGGER.info(messages.length + " messages from client are required to send.");
                                }
                            } catch (Exception e) {
                                LOGGER.error("Message manufacture caught an exception.", e);
                            }
                        }
                    }, 3000, 100, TimeUnit.MILLISECONDS);
        } else {
            long start = System.currentTimeMillis();
            Message[] messages = buildMessages(count);
            producer.send(messages);
            LOGGER.info("Messages are sent in async manner. Cost " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    public static Message[] buildMessages(int n) {
        Message[] messages = new Message[n];
        for (int i = 0; i < n; i++) {
            byte[] messageBody = buildBody();
            messages[i] = new Message(TOPIC, messageBody);
            messages[i].putUserProperty("sequenceId", String.valueOf(SEQUENCE_GENERATOR.incrementAndGet()));
        }
        return messages;
    }

    private static byte[] buildBody(){
        Map<String, String> map = new HashMap<String, String>();
        long ind = INDEX.incrementAndGet();
        map.put("transaction_id", UUID.randomUUID().toString());
        map.put("platform_id", pId.get((int) (ind % pId.size())));
        map.put("affiliate_id", affId.get((int) (ind % affId.size())));
        if ((ind/80000)%2 == 0)
            map.put("offer_id", offId.get((int) (ind % offId.size())));
        else
            map.put("offer_id", offId2.get((int) (ind % offId.size())));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        map.put("click_time", format.format(new Date()));
        map.put("status", status.get((int) (ind % status.size())));
        if (ind % 7 == 0) {
            if (ind % 35 == 0 && code.get((int) (ind % code.size())).startsWith("V")){
                    map.put("event_code", "V000");
                    map.put("memo",  "正常记录" );
            }else {
                map.put("event_code", code.get((int) (ind % code.size())));
                map.put("memo", code.get((int) (ind % code.size())).endsWith("0") ? "正常记录" : "错误记录");
            }
        }else {
            map.put("event_code", "C000");
            map.put("memo",  "正常记录" );
        }

        return JSON.toJSONString(map).getBytes();
    }
}

package com.alibaba.rocketmq.remoting.statistics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LatencyStatisticsItem {

    private final String reportName;

    private final AtomicReference<AtomicLong[]> items;

    public LatencyStatisticsItem(final String reportName) {
        this.reportName = reportName;
        this.items = new AtomicReference<>();
        rotate();
    }

    public AtomicLong[] rotate() {
        AtomicLong[] round = new AtomicLong[16];
        for (int i = 0; i < round.length; i++) {
            round[i] = new AtomicLong(0L);
        }
        return items.getAndSet(round);
    }

    public void add(long interval) {
        if (interval <= 1) {
            items.get()[0].incrementAndGet();
            return;
        }

        if (interval <= 10) {
            items.get()[1].incrementAndGet();
            return;
        }

        if (interval <= 20) {
            items.get()[2].incrementAndGet();
            return;
        }

        if (interval <= 50) {
            items.get()[3].incrementAndGet();
            return;
        }

        if (interval <= 70) {
            items.get()[4].incrementAndGet();
            return;
        }

        if(interval <= 100) {
            items.get()[5].incrementAndGet();
            return;
        }

        if(interval <= 200) {
            items.get()[6].incrementAndGet();
            return;
        }

        if(interval <= 300) {
            items.get()[7].incrementAndGet();
            return;
        }

        if(interval <= 400) {
            items.get()[8].incrementAndGet();
            return;
        }

        if(interval <= 500) {
            items.get()[9].incrementAndGet();
            return;
        }

        if(interval <= 600) {
            items.get()[10].incrementAndGet();
            return;
        }

        if(interval <= 700) {
            items.get()[11].incrementAndGet();
            return;
        }

        if(interval <= 800) {
            items.get()[12].incrementAndGet();
            return;
        }

        if(interval <= 900) {
            items.get()[13].incrementAndGet();
            return;
        }

        if(interval <= 1000) {
            items.get()[14].incrementAndGet();
            return;
        }

        items.get()[15].incrementAndGet();
    }

    public String report(AtomicLong[] data) {
        Long[] array = new Long[data.length];
        for (int i = 0; i < data.length; i++) {
            array[i] = data[i].get();
        }

        String template = "%d <=1ms, %d 1~10ms, %d 10~20ms, %d 20~50ms, %d 50~70ms, %d 70~100ms, " +
            "%d 100~200ms, %d 200~300ms, %d 300~400ms, %d 400~500ms, %d 500~600ms, %d 600~700ms, %d 700~800ms, " +
            "%d 800~900ms, %d 900~1000ms, %d >1000ms";
        return String.format("[%s] ", reportName) + String.format(template, array);
    }

    public String rotateAndReport() {
        return report(rotate());
    }
}

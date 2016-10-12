package com.alibaba.rocketmq.client.producer.selector;

public enum Region {
    US_EAST(1),
    US_WEST(3),
    SINGAPORE(2),
    FRANKFURT(9),
    ANY(100),
    SAME(101);
    private int index;

    Region(int index) {
        this.index = index;
    }

    public static Region parse(int index) {

        for (Region region : Region.values()) {
            if (region.index == index) {
                return region;
            }
        }

        return Region.ANY;
    }

    public int getIndex() {
        return index;
    }
}

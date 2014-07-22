package com.alibaba.rocketmq.storm.domain;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public enum Spouts {
    SIMPLE("simple"),
    BATCH("batch"),
    STREAM("stream");

    private String value;

    Spouts(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    private static Map<String, Spouts> stringToEnum = Maps.newHashMap();

    static {
        for (Spouts spout : values()) {
            stringToEnum.put(spout.getValue(), spout);
        }
    }

    public static Spouts fromString(String value) {
        return stringToEnum.get(value);
    }
}

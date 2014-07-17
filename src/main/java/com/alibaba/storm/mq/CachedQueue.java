package com.alibaba.storm.mq;

import java.util.TreeMap;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public class CachedQueue {

    private final TreeMap<Long, MessageExt> msgCachedTable = Maps.newTreeMap();

    public TreeMap<Long, MessageExt> getMsgCachedTable() {
        return msgCachedTable;
    }
}

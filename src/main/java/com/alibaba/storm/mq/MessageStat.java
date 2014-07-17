package com.alibaba.storm.mq;

import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Von Gosling
 */
public class MessageStat implements Serializable {
    private static final long serialVersionUID = 1277714452693486955L;

    private MessageQueue      mq;
    private String            topic;

    private AtomicInteger     failureTimes     = new AtomicInteger(0);
    private long              emitMs           = System.currentTimeMillis();

    public MessageStat(String topic) {
        this.topic = topic;
    }

    public MessageStat(MessageQueue mq) {
        this(mq, 0);
    }

    public MessageStat(MessageQueue mq, int failureTimes) {
        this.mq = mq;
        this.failureTimes = new AtomicInteger(failureTimes);
    }

    public void updateEmitMs() {
        this.emitMs = System.currentTimeMillis();
    }

    public MessageQueue getMq() {
        return mq;
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public long getEmitMs() {
        return emitMs;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

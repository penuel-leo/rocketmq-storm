package com.alibaba.rocketmq.storm.domain;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Von Gosling
 */
public class RocketMQConfig implements Serializable {
    private static final long serialVersionUID        = 4157424979688590880L;

    public static final int   DEFAULT_FAIL_TIME       = 5;
    public static final int   DEFAULT_QUEUE_SIZE      = 1024;
    public static final int   DEFAULT_BATCH_MSG_NUM   = 32;
    public static final int   DEFAULT_PULL_THREAD_NUM = 4;

    private final String      consumerGroup;
    private final String      topic;
    private final String      topicTag;

    /**
     * consume strictly order, will affect performance
     */
    private boolean           ordered;

    /**
     * The max allowed failures for one single message, skip the failure message
     * if excesses
     * <p/>
     * -1 means try again until success
     */
    private int               maxFailTimes            = DEFAULT_FAIL_TIME;

    /**
     * Local messages threshold, trigger flow control if excesses
     */
    private int               queueSize               = DEFAULT_QUEUE_SIZE;

    /**
     * fetch messages size from local queue, default 1
     */
    private int               consumeBatchMsgNum      = DEFAULT_BATCH_MSG_NUM;

    /**
     * pull message size from server for every batch
     */
    private int               pullBatchMsgNum         = DEFAULT_BATCH_MSG_NUM;

    /**
     * pull interval(ms) from server for every batch
     */
    private long              pullInterval            = 0;

    /**
     * pull threads num
     */
    private int               pullThreadNum           = DEFAULT_PULL_THREAD_NUM;

    /**
     * Consumer start time Null means start from the last consumption
     * time(CONSUME_FROM_LAST_OFFSET)
     */
    private Long              startTimeStamp;

    private Properties        peroperties;

    public RocketMQConfig(String consumerGroup, String topic, String topicTag) {
        super();
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.topicTag = topicTag;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public int getMaxFailTimes() {
        return maxFailTimes;
    }

    public void setMaxFailTimes(int maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getConsumeBatchMsgNum() {
        return consumeBatchMsgNum;
    }

    public void setConsumeBatchMsgNum(int consumeBatchMsgNum) {
        this.consumeBatchMsgNum = consumeBatchMsgNum;
    }

    public int getPullBatchMsgNum() {
        return pullBatchMsgNum;
    }

    public void setPullBatchMsgNum(int pullBatchMsgNum) {
        this.pullBatchMsgNum = pullBatchMsgNum;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public Long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(Long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public Properties getPeroperties() {
        return peroperties;
    }

    public void setPeroperties(Properties peroperties) {
        this.peroperties = peroperties;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public String getTopicTag() {
        return topicTag;
    }

    public int getPullThreadNum() {
        return pullThreadNum;
    }

    public void setPullThreadNum(int pullThreadNum) {
        this.pullThreadNum = pullThreadNum;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}

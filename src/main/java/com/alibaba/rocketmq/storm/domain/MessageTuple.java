package com.alibaba.rocketmq.storm.domain;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Von Gosling
 */
public class MessageTuple {
    private final static long      WAIT_TIMEOUT = 4;

    private final UUID             batchId;
    private final List<MessageExt> msgList;
    private final MessageQueue     mq;
    private final AtomicInteger    failureTimes;
    private CountDownLatch         latch;
    private boolean                isSuccess;

    public MessageTuple(List<MessageExt> msgList, MessageQueue mq) {
        this.msgList = msgList;
        this.mq = mq;

        this.batchId = UUID.randomUUID();
        this.failureTimes = new AtomicInteger(0);

        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(WAIT_TIMEOUT, TimeUnit.HOURS);
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public String getMessageQueue() {
        if (mq == null) {
            return null;
        }
        return mq.toString();
    }

    public MessageStat buildMsgAttribute() {
        return new MessageStat(mq, failureTimes.get());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MessageTuple ");
        sb.append(",batchId:").append(batchId);
        sb.append(",failureTimes:").append(failureTimes);
        sb.append(",msgs:").append(msgList);

        return sb.toString();

    }

    public String toSimpleString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MessageQueue:").append(getMessageQueue());
        sb.append(",batchId:").append(batchId);
        sb.append(",failureTimes:").append(failureTimes);

        return sb.toString();
    }

}

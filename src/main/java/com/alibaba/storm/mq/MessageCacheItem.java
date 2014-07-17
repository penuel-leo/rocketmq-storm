package com.alibaba.storm.mq;

import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.UUID;

/**
 * @author Von Gosling
 */
public class MessageCacheItem {
    private final UUID        batchId;
    private final MessageExt  msg;
    private final MessageStat msgStat;

    public MessageCacheItem(UUID batchId, MessageExt msg, MessageStat msgStat) {
        this.batchId = batchId;
        this.msg = msg;
        this.msgStat = msgStat;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public MessageStat getMsgStat() {
        return msgStat;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public String toSimpleString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MessageId:").append(msg.getMsgId());
        sb.append(",msgStat:").append(msgStat);

        return sb.toString();
    }
}

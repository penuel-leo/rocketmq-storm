package com.alibaba.rocketmq.storm.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @author Von Gosling
 */
public class MessageConsumer implements Serializable {
    private static final long                 serialVersionUID = 4641537253577312163L;

    private static final Logger               LOG              = LoggerFactory
                                                                       .getLogger(MessageConsumer.class);

    protected final RocketMQConfig            config;

    protected transient DefaultMQPushConsumer consumer;

    public MessageConsumer(RocketMQConfig config) {
        this.config = config;
    }

    public void init(MessageListener listener, String instanceName) throws Exception {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}", new Object[] {
                instanceName, config });

        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(config.getTopic(), config.getTopicTag());
        consumer.registerMessageListener(listener);

        consumer.setPullThresholdForQueue(config.getQueueSize());
        consumer.setConsumeMessageBatchMaxSize(config.getConsumeBatchMsgNum());
        consumer.setPullBatchSize(config.getPullBatchMsgNum());
        consumer.setPullInterval(config.getPullInterval());
        consumer.setConsumeThreadMin(config.getPullThreadNum());
        consumer.setConsumeThreadMax(config.getPullThreadNum());

        if (config.getStartTimeStamp() != null) {
            Date date = new Date(config.getStartTimeStamp());
            MQHelper.resetOffsetByTimestamp(MessageModel.CLUSTERING, instanceName,
                    config.getConsumerGroup(), config.getTopic(), config.getStartTimeStamp());

            LOG.info("Successfully reset offset to {}", date);
        }

        this.consumer.start();

        LOG.info("Init consumer successfully,instanceName->{},configuration->{}", new Object[] {
                instanceName, config });
    }

    public void cleanup() {
        consumer.shutdown();

        LOG.info("Successfully shutdown consumer {}", config);
    }

    public Set<MessageQueue> listAllQueues() throws MQClientException {
        if (consumer == null) {
            throw new MQClientException("Consumer isn't ready", new RuntimeException());
        }

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(config.getTopic());

        return mqs;
    }

    public void pause() {
        consumer.suspend();

        LOG.info("Pause consumer");
    }

    public void resume() {
        consumer.resume();

        LOG.info("Resume consumer");
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}

package com.alibaba.rocketmq.storm;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.guava.collect.Lists;
import org.apache.storm.guava.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

/**
 * @author Von Gosling
 */
public class MessagePullConsumer implements Serializable, MessageQueueListener {
    private static final long               serialVersionUID   = 4641537253577312163L;

    private static final Logger             LOG                = LoggerFactory
                                                                       .getLogger(MessagePullConsumer.class);

    private final RocketMQConfig            config;

    private transient DefaultMQPullConsumer consumer;

    private Map<String, List<MessageQueue>> topicQueueMappings = Maps.newHashMap();

    public Map<String, List<MessageQueue>> getTopicQueueMappings() {
        return topicQueueMappings;
    }

    public MessagePullConsumer(RocketMQConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null,
                false);

        consumer.registerMessageQueueListener(config.getTopic(), this);

        this.consumer.start();

        LOG.info("Init consumer successfully,configuration->{} !", config);
    }

    public void shutdown() {
        consumer.shutdown();

        LOG.info("Successfully shutdown consumer {} !", config);
    }

    public void suspend() {
        LOG.info("Nothing to do for pull consumer !");
    }

    public void resume() {
        LOG.info("Nothing to do for pull consumer !");
    }

    public DefaultMQPullConsumer getConsumer() {
        return consumer;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll,
                                    Set<MessageQueue> mqDivided) {
        if (topicQueueMappings.get(topic) != null) {

            LOG.info("Queue changed from {} to {} !", mqAll, mqDivided);

            topicQueueMappings.put(topic, Lists.newArrayList(mqDivided));
        }
    }
}

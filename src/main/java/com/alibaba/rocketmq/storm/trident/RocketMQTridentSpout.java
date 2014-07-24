package com.alibaba.rocketmq.storm.trident;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.storm.MessageConsumerManager;
import com.alibaba.rocketmq.storm.domain.BatchMessage;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

/**
 * @author Von Gosling
 */
public class RocketMQTridentSpout implements IPartitionedTridentSpout<BatchMessage> {
    private static final long                                      serialVersionUID   = -2912595422083263970L;
    private static final Logger                                    LOG                = LoggerFactory
                                                                                              .getLogger(RocketMQTridentSpout.class);

    private static final ConcurrentMap<String, List<MessageQueue>> cachedMessageQueue = new MapMaker()
                                                                                              .makeMap();
    private RocketMQConfig                                         config;
    private static DefaultMQPullConsumer                           consumer;

    public RocketMQTridentSpout() {
    }

    public RocketMQTridentSpout(RocketMQConfig config) throws MQClientException {
        this.config = config;
        consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null,
                false);
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.start();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(consumer);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IPartitionedTridentSpout.Emitter<BatchMessage> getEmitter(Map conf,
                                                                     TopologyContext context) {
        return new Emitter(consumer);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tId", "MessageExt");
    }

    class Coordinator implements IPartitionedTridentSpout.Coordinator {

        DefaultMQPullConsumer consumer;

        public Coordinator(DefaultMQPullConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public long numPartitions() {
            List<MessageQueue> queue = Lists.newArrayList();
            try {
                queue = getMessageQueue(config.getTopic());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return queue.size();
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {
            LOG.info("close coordinator!");
        }

    }

    class Emitter implements IPartitionedTridentSpout.Emitter<BatchMessage> {

        DefaultMQPullConsumer consumer;

        public Emitter(DefaultMQPullConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public BatchMessage emitPartitionBatchNew(TransactionAttempt tx,
                                                  TridentCollector collector, int partition,
                                                  BatchMessage lastPartitionMeta) {
            long index = 0;
            BatchMessage batchMessages = null;
            MessageQueue mq = null;
            try {
                if (lastPartitionMeta == null) {
                    index = consumer.fetchConsumeOffset(mq, true);
                    index = index == -1 ? 0 : index;
                } else {
                    index = lastPartitionMeta.getNextOffset();
                }

                mq = getMessageQueue(config.getTopic()).get(partition);

                PullResult result = consumer.pullBlockIfNotFound(mq, config.getTopicTag(), index,
                        32);
                List<MessageExt> msgs = result.getMsgFoundList();
                if (null != msgs && msgs.size() > 0) {
                    batchMessages = new BatchMessage(msgs, mq);
                    consumer.updateConsumeOffset(mq, result.getMaxOffset());
                    for (MessageExt msg : msgs) {
                        collector.emit(Lists.newArrayList(tx, msg));
                    }
                }
            } catch (MQClientException | RemotingException | MQBrokerException
                    | InterruptedException e) {
                e.printStackTrace();
            }
            return batchMessages;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
                                       int partition, BatchMessage partitionMeta) {
            MessageQueue mq = null;
            try {
                mq = getMessageQueue(config.getTopic()).get(partition);

                PullResult result = consumer.pullBlockIfNotFound(mq, config.getTopicTag(),
                        partitionMeta.getOffset(), partitionMeta.getMsgList().size());
                List<MessageExt> msgs = result.getMsgFoundList();
                if (null != msgs && msgs.size() > 0) {
                    consumer.updateConsumeOffset(mq, partitionMeta.getNextOffset());
                    for (MessageExt msg : msgs) {
                        collector.emit(Lists.newArrayList(tx, msg));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() {

        }
    }

    private List<MessageQueue> getMessageQueue(String topic) throws MQClientException {
        List<MessageQueue> cachedQueue = Lists.newArrayList();
        cachedQueue = cachedMessageQueue.get(config.getTopic());
        if (cachedQueue == null) {
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(config.getTopic());
            cachedQueue = Lists.newArrayList(mqs);
            cachedMessageQueue.put(config.getTopic(), cachedQueue);

        }
        return cachedQueue;
    }
}

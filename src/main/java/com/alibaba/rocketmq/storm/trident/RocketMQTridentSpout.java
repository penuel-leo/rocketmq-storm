package com.alibaba.rocketmq.storm.trident;

import java.util.Map;

import storm.trident.spout.ITridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

/**
 * @author Von Gosling
 */
public class RocketMQTridentSpout implements ITridentSpout<Long> {

    private static final long      serialVersionUID = -7610823034112048210L;

    private BatchCoordinator<Long> coordinator      = new DefaultCoordinator();
    private Emitter<Long>          emitter          = new DefaultEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }

}

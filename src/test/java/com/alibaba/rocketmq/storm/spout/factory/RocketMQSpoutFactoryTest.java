package com.alibaba.rocketmq.storm.spout.factory;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.alibaba.rocketmq.storm.domain.Spouts;

import backtype.storm.topology.IRichSpout;

/**
 * @author Von Gosling 2014年7月22日 上午10:11:23
 */
public class RocketMQSpoutFactoryTest {

    @Test(expected = java.lang.NullPointerException.class)
    public void getNullSpoutTest() throws Exception {
        RocketMQSpoutFactory.getSpout(null);
    }

    @Test
    public void getSpoutTest() throws Exception {
        IRichSpout richSpout = RocketMQSpoutFactory.getSpout(Spouts.SIMPLE.getValue());
        assertThat(richSpout, isA(backtype.storm.topology.IRichSpout.class));
        assertThat(richSpout, instanceOf(com.alibaba.rocketmq.storm.spout.SimpleMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(Spouts.BATCH.getValue());
        assertThat(richSpout, instanceOf(com.alibaba.rocketmq.storm.spout.BatchMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(Spouts.STREAM.getValue());
        assertThat(richSpout, instanceOf(com.alibaba.rocketmq.storm.spout.StreamMessageSpout.class));
    }
}

package com.alibaba.rocketmq.storm.spout.factory;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import backtype.storm.topology.IRichSpout;

import com.alibaba.rocketmq.storm.domain.Spouts;
import com.alibaba.rocketmq.storm.spout.BatchMessageSpout;
import com.alibaba.rocketmq.storm.spout.SimpleMessageSpout;
import com.alibaba.rocketmq.storm.spout.StreamMessageSpout;

/**
 * @author Von Gosling
 */
public class RocketMQSpoutFactoryTest {

    @Test(expected = java.lang.NullPointerException.class)
    public void getNullSpoutTest() throws Exception {
        RocketMQSpoutFactory.getSpout(null);
    }

    @Test
    public void getSpoutTest() throws Exception {
        IRichSpout richSpout = RocketMQSpoutFactory.getSpout(Spouts.SIMPLE.getValue());
        assertThat(richSpout, isA(IRichSpout.class));
        assertThat(richSpout, instanceOf(SimpleMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(Spouts.BATCH.getValue());
        assertThat(richSpout, instanceOf(BatchMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(Spouts.STREAM.getValue());
        assertThat(richSpout, instanceOf(StreamMessageSpout.class));
    }
}

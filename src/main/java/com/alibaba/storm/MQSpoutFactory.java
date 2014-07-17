package com.alibaba.storm;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.topology.IRichSpout;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.storm.mq.MQConfig;
import com.alibaba.storm.spout.BatchMessageSpout;
import com.alibaba.storm.spout.DefaultMessageSpout;
import com.alibaba.storm.spout.SimpleMessageSpout;
import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public class MQSpoutFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MQSpoutFactory.class);

    public static Config initConfig(String configFile) {
        Config config = new Config();
        Properties prop = new Properties();
        InputStream input = MQSpoutFactory.class.getClassLoader().getResourceAsStream(configFile);
        try {
            prop.load(input);
        } catch (IOException e) {
            LOG.warn("SpoutFactory Load Properties error", e);
        }
        for (Entry<Object, Object> entry : prop.entrySet()) {
            config.put((String) entry.getKey(), entry.getValue());
        }
        return config;
    }

    public static Map<String, IRichSpout> createMqSpouts(Config conf) {
        Map<String, IRichSpout> mqSpouts = Maps.newHashMap();
        String topic = (String) conf.get("mq.topic");
        String consumerGroup = (String) conf.get("mq.consumer.group");
        String subExpress = (String) conf.get("mq.subexpress");
        int spoutType = JStormUtils.parseInt(conf.get("spout.type"), 2);
        boolean ordered = JStormUtils.parseBoolean(conf.get("spout.ordered"), false);
        MQConfig config = new MQConfig(consumerGroup, topic, subExpress);
        config.setOrdered(ordered);

        /**
         * if set max fail times as -1, it will retry failure message until
         * success
         */
        config.setMaxFailTimes(3);

        /**
         * local queue size, bigger queue size, better performance but it will
         * cost more performance
         */
        config.setQueueSize(512);

        String consumeStartDate = (String) conf.get("mq.consumer.start.time");
        if (consumeStartDate != null) {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = simpleDateFormat.parse(consumeStartDate);
                if (date != null) {
                    long startMs = date.getTime();
                    config.setStartTimeStamp(startMs);
                    LOG.info("Setting consumer start time to " + date);
                }

            } catch (Exception e) {
                LOG.info("Failed to set consumer start time", e);
            }
        }

        IRichSpout spout = null;
        switch (spoutType) {
            case 0:
                /**
                 * SimpleSpout performance is best, but when kill topology, it
                 * will discard failure message
                 */
                spout = new SimpleMessageSpout(config);
                break;
            case 1:
                /**
                 * BatchSpout send batch messages
                 */
                spout = new BatchMessageSpout(config);
                break;
            case 2:
                /**
                 * DefaultMessageSpout send message one by one
                 */
                spout = new DefaultMessageSpout(config);
            default:
                break;
        }
        mqSpouts.put(topic, spout);
        return mqSpouts;

    }
}

package com.aliexpress.guestchat.storm;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.local.LocalCluster;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.storm.MQSpoutFactory;
import com.alibaba.storm.bolt.RocketMqBolt;

/**
 * @author Von Gosling 2014年6月30日 下午12:40:11
 */
public class DemoTopology {
    private static final Logger LOG            = LoggerFactory.getLogger(DemoTopology.class);

    private static final String BOLT_NAME      = "notifier";
    private static final String PROP_FILE_NAME = "mqspout.test.prop";

    private static Config       conf           = new Config();
    private static boolean      isLocalMode    = true;

    public static void main(String[] args) throws Exception {
        conf = MQSpoutFactory.initConfig(PROP_FILE_NAME);

        TopologyBuilder builder = setupBuilder();

        submitTopology(builder);
    }

    private static TopologyBuilder setupBuilder() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        int writerParallel = JStormUtils.parseInt(conf.get("topology.writer.parallel"), 1);

        int spoutParallel = JStormUtils.parseInt(conf.get("topology.spout.parallel"), 1);

        BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), writerParallel);

        Map<String, IRichSpout> metaSpouts = MQSpoutFactory.createMqSpouts(conf);

        for (Entry<String, IRichSpout> entry : metaSpouts.entrySet()) {

            String spoutName = entry.getKey();

            builder.setSpout(spoutName, entry.getValue(), spoutParallel);

            writerBolt.shuffleGrouping(spoutName);
        }
        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            String topologyName = String.valueOf(conf.get("topology.name"));
            StormTopology topology = builder.createTopology();

            if (isLocalMode == true) {
                LocalCluster cluster = new LocalCluster();
                conf.put(Config.STORM_CLUSTER_MODE, "local");

                cluster.submitTopology(topologyName, conf, topology);

                Thread.sleep(50000);

                cluster.killTopology(topologyName);
                cluster.shutdown();
            } else {
                conf.put(Config.STORM_CLUSTER_MODE, "distributed");
                StormSubmitter.submitTopology(topologyName, conf, topology);
            }

        } catch (AlreadyAliveException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (InvalidTopologyException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    }
}

## Description

rocketmq-storm-alibaba allows a Storm topology to consume an RocketMQ queue as an input source. It currently provides:

#### SimpleMessageSpout: 
An simple implementation of backtype.storm.topology.IRichSpout,consumes the messages one by one.full features spout implementation exception flow control function;

#### BatchMessageSpout: 
As the name implies,It handle the messages in a batch way,also with supporting reliable messages;

#### DefaultMessageSpout: 
Based on batchMessageSpout,cache batch messages and emit message one by one.It is also recommendation spout at the present stage

#### AsyncMessageSpout: 
Implementing...

## Documentation
Please look forward to!

## Code Snippet

#### private static final String BOLT_NAME      = "notifier";
    private static final String PROP_FILE_NAME = "mqspout.test.prop";

    private static Config       conf           = new Config();
    private static boolean      isLocalMode    = true;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = buildTopology(ConfigUtils.init(PROP_FILE_NAME));

        submitTopology(builder);
    }

    private static TopologyBuilder buildTopology(Config config) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        int boltParallel = NumberUtils.toInt((String) config.get("topology.bolt.parallel"), 1);

        int spoutParallel = NumberUtils.toInt((String) config.get("topology.spout.parallel"), 1);

        BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), boltParallel);

        StreamMessageSpout defaultSpout = (StreamMessageSpout) RocketMQSpoutFactory
                .getSpout(Spouts.STREAM.getValue());
        RocketMQConfig mqConig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
        defaultSpout.setConfig(mqConig);

        String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        builder.setSpout(id, defaultSpout, spoutParallel);

        writerBolt.shuffleGrouping(id);
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


## How to upload task jar
To produce a jar:

$ mvn clean install
To install in your local Maven repository:


Run it

$storm jar rocketmq-storm-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.storm.Topology


## Compatibility
#### RocketMQ 3.x

#### Jstorm 0.9.X

#### Storm 0.9.x

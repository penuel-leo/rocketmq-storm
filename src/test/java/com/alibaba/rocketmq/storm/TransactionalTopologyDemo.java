/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.alibaba.rocketmq.storm.internal.tools.ConfigUtils;
import com.alibaba.rocketmq.storm.trident.RocketMQTridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @author Von Gosling
 */
public class TransactionalTopologyDemo {

    public static final Logger LOG = LoggerFactory.getLogger(TransactionalTopologyDemo.class);

    private static final String PROP_FILE_NAME = "mqspout.test.prop";

    public static StormTopology buildTopology() throws MQClientException {
        TridentTopology topology = new TridentTopology();

        Config config = ConfigUtils.init(PROP_FILE_NAME);
        RocketMQConfig rocketMQConfig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);

        RocketMQTridentSpout spout = new RocketMQTridentSpout(rocketMQConfig);
        Stream stream = topology.newStream("rocketmq-txId", spout);
        stream.each(new Fields("message"), new BaseFilter() {
            private static final long serialVersionUID = -9056745088794551960L;

            @Override
            public boolean isKeep(TridentTuple tuple) {
                LOG.debug("Entering filter...");
                return true;
            }

        });
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(String.valueOf(conf.get("topology.name")), conf, buildTopology());

        Thread.sleep(5000);

        cluster.shutdown();
    }
}

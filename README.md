## Description

rocketmq-storm-alibaba allows a Storm topology to consume an RocketMQ queue as an input source. It currently provides:

#### SimpleMessageSpout: 
An simple implementation of backtype.storm.topology.IRichSpout,consumes the messages one by one.full features spout implementation exception flow control function;

#### BatchMessageSpout: 
As the name implies,It handle the messages in a batch way,also with supporting reliable messages;

#### StreamMessageSpout: 
Based on batchMessageSpout,cache batch messages and emit message one by one.It is also recommendation spout at the present stage

#### AsyncMessageSpout: 
Implementing...

## Documentation
Please look forward to!

## Usage
To produce a jar:

$ mvn clean install
To install in your local Maven repository:


Run it

$storm jar rocketmq-storm-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.storm.Topology


## Compatibility
#### RocketMQ 3.x

#### Jstorm 0.9.X

#### Storm 0.9.x

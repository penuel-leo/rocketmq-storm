package com.alibaba.rocketmq.storm.trident;

import java.io.Serializable;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

/**
 * @author Von Gosling
 */
public class DefaultEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = -4866485534385837298L;

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        collector.emit(null);
    }

    @Override
    public void success(TransactionAttempt tx) {

    }

    @Override
    public void close() {

    }

}

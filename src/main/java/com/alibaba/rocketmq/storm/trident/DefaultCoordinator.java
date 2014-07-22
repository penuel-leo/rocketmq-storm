package com.alibaba.rocketmq.storm.trident;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

/**
 * @author Von Gosling
 */
public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {
    private static final long   serialVersionUID = 8607677938371385006L;

    private static final Logger LOG              = LoggerFactory
                                                         .getLogger(DefaultCoordinator.class);

    @Override
    public void success(long txid) {
        LOG.debug("Successful Transaction {}", txid);
    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
        LOG.debug("Closing Transaction...");
    }

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata) {
        LOG.debug("Initializing Transaction {}", txid);
        return null;
    }

}

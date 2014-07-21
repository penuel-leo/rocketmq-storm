package com.alibaba.storm.internal.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utilities for RocketMQ spout regarding its configuration and reading values
 * from the storm configuration.
 * 
 * @author Von Gosling
 */
public abstract class ConfigUtils {
    /**
     * Configuration key prefix in storm config for rocketmq configuration
     * parameters ({@code "rocketmq."}). The prefix is stripped from all keys
     * that use it and passed to rocketmq.
     */
    public static final String CONFIG_PREFIX                 = "rocketmq.";
    /**
     * Storm configuration key pointing to a file containing rocketmq
     * configuration ({@code "rocketmq.config"}).
     */
    public static final String CONFIG_FILE                   = "rocketmq.config";
    /**
     * Storm configuration key used to determine the rocketmq topic to read from
     * ( {@code "rocketmq.spout.topic"}).
     */
    public static final String CONFIG_TOPIC                  = "rocketmq.spout.topic";
    /**
     * Default rocketmq topic to read from ({@code "rocketmq_spout_topic"}).
     */
    public static final String CONFIG_DEFAULT_TOPIC          = "rocketmq_spout_topic";
    /**
     * Storm configuration key used to determine the rocketmq consumer group (
     * {@code "rocketmq.spout.consumer.group"}).
     */
    public static final String CONFIG_CONSUMER_GROUP         = "rocketmq.spout.consumer.group";
    /**
     * Default rocketmq consumer group id (
     * {@code "rocketmq_spout_consumer_group"}).
     */
    public static final String CONFIG_DEFAULT_CONSUMER_GROUP = "rocketmq_spout_consumer_group";
    /**
     * Storm configuration key used to determine the rocketmq topic tag(
     * {@code "rocketmq.spout.topic.tag"}).
     */
    public static final String CONFIG_TOPIC_TAG              = "rocketmq.spout.topic.tag";

    /**
     * Reads configuration from a classpath resource stream obtained from the
     * current thread's class loader through
     * {@link ClassLoader#getSystemResourceAsStream(String)}.
     * 
     * @param resource The resource to be read.
     * @return A {@link java.util.Properties} object read from the specified
     *         resource.
     * @throws IllegalArgumentException When the configuration file could not be
     *             found or another I/O error occurs.
     */
    public static Properties getResource(final String resource) {
        InputStream input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resource);
        if (input == null) {
            throw new IllegalArgumentException("configuration file '" + resource
                    + "' not found on classpath");
        }

        final Properties config = new Properties();
        try {
            config.load(input);
        } catch (final IOException e) {
            throw new IllegalArgumentException("reading configuration from '" + resource
                    + "' failed", e);
        }
        return config;
    }

}

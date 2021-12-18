/*
 * Name:   Kafka Consumer Configuration
 * Author: Bhaskar S
 * Date:   11/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public final class KafkaConsumerConfig {
    public static Properties kafkaConfigurationOne(String appId) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:20001");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return config;
    }

    public static Properties kafkaConfigurationTwo(String appId, int numThr) {
        Properties config = kafkaConfigurationOne(appId);

        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThr);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/home/bswamina/Downloads/DATA/kafka/state");

        return config;
    }

    public static Properties kafkaConfigurationThree(String appId, int numThr, long totMemSz) {
        Properties config = kafkaConfigurationTwo(appId, numThr);

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, totMemSz);

        return config;
    }

    public static Properties kafkaConfigurationFour(String appId, int numThr, Serde<?> serde) {
        Properties config = kafkaConfigurationTwo(appId, numThr);

        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serde.getClass());

        return config;
    }

    public static Properties kafkaConfigurationFive(String appId, int numThr, Serde<?> serde, Class<?> clazz) {
        Properties config = kafkaConfigurationFour(appId, numThr, serde);

        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, clazz);

        return config;
    }

    private KafkaConsumerConfig() {}
}

/*
 * Name:   Two Data Streams Joiner
 * Author: Bhaskar S
 * Date:   12/11/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class StreamsJoiner {
    private static void usage() {
        System.out.printf("Usage: java %s <topic-1> <topic-2> <SSI | STL | TTO>\n", StreamsJoiner.class.getName());
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            usage();
        }

        Logger log = LoggerFactory.getLogger(StreamsJoiner.class.getName());

        StreamsConfig config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationFour(
                String.format("%s-%s", args[0], args[1]), 1, Serdes.Integer()));

        Serde<String> stringSerde = Serdes.String();
        Serde<Integer> integerSerde = Serdes.Integer();

        // Window duration - 3 secs
        Duration windowSz = Duration.ofSeconds(3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> streamOne = builder.stream(args[0], Consumed.with(stringSerde, integerSerde))
                .peek((key, metric) -> log.info(String.format(".S1> Key: %s, Metrics-1: %d", key, metric)));

        KStream<String, Integer> streamTwo = builder.stream(args[1], Consumed.with(stringSerde, integerSerde))
                .peek((key, metric) -> log.info(String.format(".S2> Key: %s, Metrics-2: %d", key, metric)));

        switch (args[2]) {
            case "SSI" -> { // KStream-KStream Inner Join
                log.info("------> KStream-KStream Inner Join");

                streamOne.join(streamTwo,
                                (oneVal, twoVal) -> String.format("Metrics-1: %d, Metrics-2: %d", oneVal, twoVal),
                                JoinWindows.of(windowSz))
                        .peek((key, value) -> log.info(String.format("SSI> Key: %s, Value: %s", key, value)));
            }
            case "STL" -> { // KStream-KTable Left Join
                log.info("------> KStream-KTable Left Join");

                KTable<String, Integer> tableTwo = streamTwo.toTable();
                tableTwo.toStream()
                        .peek((key, metric) -> log.info(String.format(".T2> Key: %s, Metrics-2: %d", key, metric)));

                streamOne.leftJoin(tableTwo,
                                (oneVal, twoVal) -> String.format("Metrics-1: %d, Metrics-2: %d", oneVal, twoVal))
                        .peek((key, value) -> log.info(String.format("STL> Key: %s, Value: %s", key, value)));
            }
            case "TTO" -> { // KTable-KTable Outer Join
                log.info("------> KTable-KTable Outer Join");

                KTable<String, Integer> tableOne = streamOne.toTable();
                tableOne.toStream()
                        .peek((key, metric) -> log.info(String.format(".T1> Key: %s, Metrics-1: %d", key, metric)));

                KTable<String, Integer> tableTwo = streamTwo.toTable();
                tableTwo.toStream()
                        .peek((key, metric) -> log.info(String.format(".T2> Key: %s, Metrics-2: %d", key, metric)));

                KTable<String, String> joined = tableOne.outerJoin(tableTwo,
                        (oneVal, twoVal) -> String.format("Metrics-1: %d, Metrics-2: %d", oneVal, twoVal));
                joined.toStream()
                        .peek((key, value) -> log.info(String.format("TTO> Key: %s, Value: %s", key, value)));
            }
        }

        Topology topology = builder.build();

        log.info(String.format("---> %s", topology.describe().toString()));

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

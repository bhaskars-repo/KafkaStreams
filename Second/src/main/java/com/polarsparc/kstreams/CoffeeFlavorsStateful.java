/*
 * Name:   Coffee Flavors (Stateful)
 * Author: Bhaskar S
 * Date:   11/23/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

public class CoffeeFlavorsStateful {
    private static final String COFFEE_FLAVORS = "coffee-flavors";

    // Arguments: [1 or 2] [0 or 1024]
    public static void main(String[] args) {
        int numThreads = 1;
        if (args.length == 1 || args.length == 2) {
            if (args[0].equals("2")) {
                numThreads = 2;
            }
        }

        long totalMemSize = 0L;
        if (args.length == 2) {
            if (args[1].equals("1024")) {
                totalMemSize = 1024L;
            }
        }

        Logger log = LoggerFactory.getLogger(CoffeeFlavorsStateful.class.getName());

        Set<String> flavorSet = Set.copyOf(Arrays.asList("caramel", "hazelnut", "mocha", "peppermint"));

        log.info(String.format("---> Num of stream threads: %d", numThreads));
        log.info(String.format("---> Total size of record cache: %d", totalMemSize));

        StreamsConfig config;
        if (totalMemSize == 0L) {
            config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationTwo(
                    "coffee-flavor-2", numThreads));
        } else {
            config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationThree(
                    "coffee-flavor-2", numThreads, totalMemSize));
        }

        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> stream = builder.stream(COFFEE_FLAVORS, Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> table = stream
                .peek((user, flavor) -> log.info(String.format("---> [Start] User: %s, Flavor: %s", user, flavor)))
                .map((user, flavor) -> KeyValue.pair(user.toLowerCase(), flavor.toLowerCase()))
                .filter((user, flavor) -> flavorSet.contains(flavor))
                .map((user, flavor) -> KeyValue.pair(flavor, user))
                .groupByKey()
                .count();

        table.toStream()
                .foreach((flavor, user) -> log.info(String.format("---> [Final] %s - %s", flavor, user)));

        Topology topology = builder.build();

        log.info(String.format("---> %s", topology.describe().toString()));

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

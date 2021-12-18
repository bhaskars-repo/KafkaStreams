/*
 * Name:   Coffee Flavors (Stateless)
 * Author: Bhaskar S
 * Date:   11/20/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

public class CoffeeFlavorsStateless {
    private static final String COFFEE_FLAVORS = "coffee-flavors";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.printf("Usage: java %s <1 or 2>\n", CoffeeFlavorsStateless.class.getName());
            System.exit(1);
        }

        int numThreads = 1;
        if (args[0].equals("2")) {
            numThreads = 2;
        }

        Logger log = LoggerFactory.getLogger(CoffeeFlavorsStateless.class.getName());

        Set<String> flavorSet = Set.copyOf(Arrays.asList("caramel", "hazelnut", "mocha", "peppermint"));

        log.info(String.format("---> Num of stream threads: %d", numThreads));

        StreamsConfig config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationTwo(
                "coffee-flavor-1", numThreads));

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(COFFEE_FLAVORS, Consumed.with(stringSerde, stringSerde));

        stream.peek((user, flavor) -> log.info(String.format("---> [Start] User: %s, Flavor: %s", user, flavor)))
                .map((user, flavor) -> KeyValue.pair(user.toLowerCase(), flavor.toLowerCase()))
                .filter((user, flavor) -> flavorSet.contains(flavor))
                .map((user, flavor) -> KeyValue.pair(flavor, user))
                .foreach((flavor, user) -> log.info(String.format("---> [Final] %s - %s", flavor, user)));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

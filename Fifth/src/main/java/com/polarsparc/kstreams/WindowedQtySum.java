/*
 * Name:   Sum quantity values using various Windowing options
 * Author: Bhaskar S
 * Date:   12/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import com.polarsparc.kstreams.model.QtyEvent;
import com.polarsparc.kstreams.serde.JsonDeserializer;
import com.polarsparc.kstreams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class WindowedQtySum {
    private static void usage() {
        System.out.printf("Usage: java %s <TM | HP | SL>\n", WindowedQtySum.class.getName());
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            usage();
        }

        String topicName = switch (args[0]) {
            case "TM" -> "tumbling-events";
            case "HP" -> "hopping-events";
            case "SL" -> "sliding-events";
            default -> null;
        };

        if (topicName == null) {
            usage();
        }

        Logger log = LoggerFactory.getLogger(WindowedQtySum.class.getName());

        log.info(String.format("---> Event type: %s", topicName));

        StreamsConfig config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationFive(topicName, 1,
                Serdes.Integer(), QtyEventTimeExtractor.class));

        JsonSerializer<QtyEvent> jsonSer = new JsonSerializer<>();
        JsonDeserializer<QtyEvent> jsonDe = new JsonDeserializer<>(QtyEvent.class);

        Serde<String> stringSerde = Serdes.String();
        Serde<QtyEvent> jsonSerde = Serdes.serdeFrom(jsonSer, jsonDe);

        // Window duration
        Duration windowSz = Duration.ofSeconds(5);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, QtyEvent> stream = builder.stream(topicName, Consumed.with(stringSerde, jsonSerde));

        KGroupedStream<String, Integer> groupedStream = stream
                .peek((key, event) -> log.info(String.format("---> [%d] >> Key: %s, Event: %s",
                        System.currentTimeMillis(), key, event)))
                .mapValues(QtyEvent::getQuantity)
                .groupByKey();

        TimeWindowedKStream<String, Integer> windowedStream = switch (args[0]) {
            default -> { // TM is the default
                TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSz);
                tumblingWindow.advanceBy(windowSz); // *IMPORTANT* for tumbling window size = advance
                yield groupedStream.windowedBy(tumblingWindow);
            }
            case "HP" -> {
                Duration advanceSz = Duration.ofSeconds(2);
                TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSz);
                hoppingWindow.advanceBy(advanceSz);
                yield groupedStream.windowedBy(hoppingWindow);
            }
            case "SL" -> {
                Duration graceSz = Duration.ofMillis(500); // Grace period
                SlidingWindows slidingWindow = SlidingWindows.ofTimeDifferenceAndGrace(windowSz, graceSz);
                yield groupedStream.windowedBy(slidingWindow);
            }
        };
        windowedStream.reduce(Integer::sum, Materialized.as(Stores.inMemoryWindowStore(topicName,
                        windowSz, windowSz, false)))
                .toStream()
                .peek((winKey, sum) -> log.info(String.format("---> [%d] >> Window: %s, Key: %s, Sum: %d",
                        System.currentTimeMillis(), winKey.window().toString(), winKey.key(), sum)));

        Topology topology = builder.build();

        log.info(String.format("---> %s", topology.describe().toString()));

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

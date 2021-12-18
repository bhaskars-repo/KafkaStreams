/*
 * Name:   Crypto Alerts Watcher (Stateful)
 * Author: Bhaskar S
 * Date:   11/25/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import com.polarsparc.kstreams.common.Crypto;
import com.polarsparc.kstreams.model.CryptoAlert;
import com.polarsparc.kstreams.serde.JsonDeserializer;
import com.polarsparc.kstreams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoAlertStateful {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(CryptoAlertStateful.class.getName());

        StreamsConfig config = new StreamsConfig(KafkaConsumerConfig.kafkaConfigurationTwo(
                "crypto-alerts-watcher", 1));

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerializer<CryptoAlert> cryptoAlertSer = new JsonSerializer<>();
        JsonDeserializer<CryptoAlert> cryptoAlertDe = new JsonDeserializer<>(CryptoAlert.class);

        Serde<String> stringSerde = Serdes.String();
        Serde<CryptoAlert> cryptoAlertSerde = Serdes.serdeFrom(cryptoAlertSer, cryptoAlertDe);

        KStream<String, CryptoAlert> stream = builder.stream(Crypto.CRYPTO_ALERTS_TOPIC,
                Consumed.with(stringSerde, cryptoAlertSerde));

        stream.peek((symbol, alert) -> log.info(String.format("---> [Start] Symbol: %s, Alert: %s",
                        symbol, alert.toString())))
                .filter((symbol, alert) -> !alert.up() && alert.change() > 25) // crypto is down greater that 25%
                .groupByKey()
                .aggregate(
                        () -> null, // Initializer
                        (symbol, alert, agg) -> {
                            CryptoAlert last = agg;
                            if (last == null) {
                                last = alert;
                            } else {
                                if (alert.change() >= last.change()) {
                                    last = alert;
                                }
                            }
                            return last;
                        }, // Aggregator
                        Materialized.with(stringSerde, cryptoAlertSerde) // Store
                )
                .toStream()
                .peek((symbol, alert) -> log.info(String.format("---> [Final] Symbol: %s, Alert: %s",
                        symbol, alert.toString())));

        Topology topology = builder.build();

        log.info(String.format("---> %s", topology.describe().toString()));

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

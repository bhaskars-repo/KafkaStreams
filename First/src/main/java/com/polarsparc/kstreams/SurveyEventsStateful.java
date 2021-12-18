/*
 * Name:   Survey Events (Stateful)
 * Author: Bhaskar S
 * Date:   11/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

public class SurveyEventsStateful {
    private static final String SURVEY_EVENT = "survey-event";

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(SurveyEventsStateful.class.getName());

        Set<String> langSet = Set.copyOf(Arrays.asList("Go", "Java", "Python"));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream(SURVEY_EVENT);

        KTable<String, Long> table = input.filter((user, lang) -> langSet.contains(lang))
                .map((user, lang) -> KeyValue.pair(lang.toLowerCase(), user.toLowerCase()))
                .groupByKey()
                .count();

        table.toStream()
                .foreach((lang, count) -> log.info(String.format("%s - %d", lang, count)));

        KafkaStreams streams = new KafkaStreams(builder.build(),
                KafkaConsumerConfig.kafkaConfigurationOne("survey-event-2"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

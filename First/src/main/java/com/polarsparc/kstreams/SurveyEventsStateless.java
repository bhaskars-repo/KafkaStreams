/*
 * Name:   Survey Events (Stateless)
 * Author: Bhaskar S
 * Date:   11/10/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

public class SurveyEventsStateless {
    private static final String SURVEY_EVENT = "survey-event";

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(SurveyEventsStateless.class.getName());

        Set<String> langSet = Set.copyOf(Arrays.asList("Go", "Java", "Python"));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(SURVEY_EVENT);

        stream.filter((user, lang) -> langSet.contains(lang))
                .map((user, lang) -> KeyValue.pair(lang.toLowerCase(), user.toLowerCase()))
                .foreach((lang, user) -> log.info(String.format("%s - %s", lang, user)));

        KafkaStreams streams = new KafkaStreams(builder.build(),
                KafkaConsumerConfig.kafkaConfigurationOne("survey-event-1"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

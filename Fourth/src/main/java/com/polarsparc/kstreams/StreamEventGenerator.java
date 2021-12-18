/*
 * Name:   Stream Data Event Generator
 * Author: Bhaskar S
 * Date:   12/04/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class StreamEventGenerator {
    private final static int MAX_EVENTS = 10;
    private final static int MAX_GAP = 3000; // 3 seconds = 3000 ms

    private static final Logger log = LoggerFactory.getLogger(StreamEventGenerator.class.getName());

    private final static Random random = new Random(1001);

    private final static List<String> keysList = Arrays.asList("A", "M", "S", "B", "N", "T");
    private final static List<Integer> valuesList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

    private static KafkaProducer<String, Integer> createEventProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:20001");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    private static void generateDataEvent(boolean flag, String topic, Producer<String, Integer> producer) {
        log.info(String.format("   ---> Topic: %s", topic));

        int cnt = random.nextInt(MAX_EVENTS);
        int gap = random.nextInt(MAX_GAP);

        log.info(String.format("   ---> Events Count: %d", cnt));

        for (int i = 1; i <= cnt; i++) {
            int ki = random.nextInt(keysList.size());
            int vi = random.nextInt(valuesList.size());

            String key = keysList.get(ki);
            Integer value = valuesList.get(vi);

            log.info(String.format("   ---> [%d] Key: %s, Value: %d", i, key, value));

            if (!flag) {
                try {
                    producer.send(new ProducerRecord<>(topic, key, value)).get();
                }
                catch (Exception ex) {
                    log.error(ex.getMessage());
                }
            }
        }

        try {
            Thread.sleep(gap);
        }
        catch (Exception ignore)  {
        }

        log.info(String.format("   ---> Sleep Gap: %d", gap));
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.printf("Usage: java %s <topic-name> [--dry-run]\n", StreamEventGenerator.class.getName());
            System.exit(1);
        }

        boolean dryRun = args.length == 2 && args[1].equalsIgnoreCase("--dry-run");

        Producer<String, Integer> producer = null;
        if (!dryRun) {
            producer = createEventProducer();
        }

        for (int i = 1; i <= 5; i++) {
            log.info(String.format("---------> Iteration: %d", i));

            generateDataEvent(dryRun, args[0], producer);
        }

        if (!dryRun) {
            producer.close();
        }
    }
}

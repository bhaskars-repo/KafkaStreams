/*
 * Name:   Stream Data Event Generator for 2 Topics
 * Author: Bhaskar S
 * Date:   12/11/2021
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

public class TwoMetricsGenerator {
    private static final Logger log = LoggerFactory.getLogger(TwoMetricsGenerator.class.getName());

    private final static List<String> keysList = Arrays.asList("A", "M", "S", "B", "N", "T");

    private final static Random random = new Random(1001);

    private static KafkaProducer<String, Integer> createMetricsProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:20001");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    private static void generateMetricEvent(boolean flag, int type, String topic, Producer<String, Integer> producer) {
        int ki = random.nextInt(keysList.size());

        String key = keysList.get(ki);

        int value;
        if (type == 1) {
            value = random.nextInt(101);
        } else {
            value = random.nextInt(4, 32);
        }

        log.info(String.format("---> [%d] Topic: %s, Key: %s, Value: %d", type, topic, key, value));

        if (!flag) {
            try {
                producer.send(new ProducerRecord<>(topic, key, value)).get();
            }
            catch (Exception ex) {
                log.error(ex.getMessage());
            }
        }

        try {
            Thread.sleep(500);
        }
        catch (Exception ignore)  {
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.printf("Usage: java %s <topic-1> <topic-2> [--dry-run]\n", TwoMetricsGenerator.class.getName());
            System.exit(1);
        }

        boolean dryRun = args.length == 3 && args[2].equalsIgnoreCase("--dry-run");

        Producer<String, Integer> producer = null;
        if (!dryRun) {
            producer = createMetricsProducer();
        }

        class TaskOne extends Thread {
            final Producer<String, Integer> kp;

            TaskOne(Producer<String, Integer> kp) {
                this.kp = kp;
            }

            @Override
            public void run() {
                for (int i = 1; i <= 10; i++) {
                    generateMetricEvent(dryRun, 1, args[0], kp);
                }
            }
        }

        class TaskTwo extends Thread {
            final Producer<String, Integer> kp;

            TaskTwo(Producer<String, Integer> kp) {
                this.kp = kp;
            }

            @Override
            public void run() {
                for (int i = 1; i <= 10; i++) {
                    generateMetricEvent(dryRun, 2, args[1], kp);
                }
            }
        }

        TaskOne t1 = new TaskOne(producer);
        TaskTwo t2 = new TaskTwo(producer);

        t1.start();
        t2.start();

        try {
            t1.join();
        }
        catch (Exception ex) {
            // Ignore
        }

        try {
            t2.join();
        }
        catch (Exception ex) {
            // Ignore
        }

        if (!dryRun) {
            producer.close();
        }
    }
}

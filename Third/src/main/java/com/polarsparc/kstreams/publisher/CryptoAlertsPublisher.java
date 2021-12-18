/*
 * Name:   Crypto Alerts Publisher
 * Author: Bhaskar S
 * Date:   11/24/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.publisher;

import com.polarsparc.kstreams.common.Crypto;
import com.polarsparc.kstreams.model.CryptoAlert;
import com.polarsparc.kstreams.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CryptoAlertsPublisher {
    private static final Logger log = LoggerFactory.getLogger(CryptoAlertsPublisher.class.getName());

    private static KafkaProducer<String, CryptoAlert> createCryptoAlertsProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:20001");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    public static void main(String[] args) {
        Producer<String, CryptoAlert> producer = createCryptoAlertsProducer();

        for (int i = 1; i <= 10; i++) {
            CryptoAlert alert = Crypto.generateNextCryptoAlert();
            try {
                log.info(String.format("---> [%02d] Crypto alert: %s", i, alert));

                ProducerRecord<String, CryptoAlert> record = new ProducerRecord<>(Crypto.CRYPTO_ALERTS_TOPIC,
                        alert.name(),
                        alert);

                producer.send(record).get();
            }
            catch (Exception ex) {
                log.error(ex.getMessage());
            }
        }

        producer.close();
    }
}

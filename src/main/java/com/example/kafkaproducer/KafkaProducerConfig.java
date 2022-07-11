package com.example.kafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerConfig {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.74:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        for (int i = 0 ; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", String.valueOf(i), "record_test_"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received record metadata : \n" +
                                "Topic: " + recordMetadata.topic() +
                                ", Partition: " + recordMetadata.partition() +
                                ", Offset: " + recordMetadata.offset());
                    } else {
                        logger.error("Error", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();


    }
}
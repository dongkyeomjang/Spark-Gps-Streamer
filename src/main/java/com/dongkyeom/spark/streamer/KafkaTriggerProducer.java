package com.dongkyeom.spark.streamer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaTriggerProducer {
    private final KafkaProducer<String, String> producer;

    public KafkaTriggerProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:29092,kafka-2:29093,kafka-3:29094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void send(String key, String value) {
        producer.send(new ProducerRecord<>("create-route-trigger", key, value));
    }

    public void close() {
        producer.close();
    }
}

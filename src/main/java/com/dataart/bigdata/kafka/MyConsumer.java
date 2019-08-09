package com.dataart.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static com.dataart.bigdata.streaming.MeasurementProcessor.INPUT;

public class MyConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "com.dataart.bigdata.kafka.MeasurementDeserializer");

        KafkaConsumer<String, Measurement> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(INPUT));

        while (true) {
            ConsumerRecords<String, Measurement> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Measurement> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

}

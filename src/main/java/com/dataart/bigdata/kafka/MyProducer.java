package com.dataart.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.dataart.bigdata.streaming.MeasurementProcessor.INPUT;

public class MyProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.dataart.bigdata.kafka.MeasurementSerializer");

        Producer<String, Measurement> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                producer.close();
            }
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<?> sender = executor.submit(() -> {
            int i = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String device = i % 2 == 0 ? "temp sensor" : "pressure sensor";
                Measurement value = new Measurement(device, System.currentTimeMillis(), i);
                producer.send(new ProducerRecord(INPUT, value));
                i++;
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        });

        System.out.println("Press any key to exit");

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        sender.cancel(true);
        executor.shutdown();
        try {
            executor.awaitTermination(1,TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}

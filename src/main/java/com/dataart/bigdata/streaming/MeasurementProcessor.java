package com.dataart.bigdata.streaming;

import com.dataart.bigdata.kafka.Measurement;
import com.dataart.bigdata.kafka.MeasurementDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


public class MeasurementProcessor {

    public static final String INPUT = "input";
    public static final String OUTPUT = "output";

    private static HashMap<String, Object> consumerParams;
    private static Properties props;

    static {
        consumerParams = new HashMap<>();
        consumerParams.put("bootstrap.servers", "localhost:9092");
        consumerParams.put("group.id", "group");
        consumerParams.put("enable.auto.commit", false);
        consumerParams.put("auto.offset.reset", "latest");
        consumerParams.put("key.deserializer", StringDeserializer.class);
        consumerParams.put("value.deserializer", MeasurementDeserializer.class);

        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Measurement Processor");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        List<String> topics = Collections.singletonList(INPUT);

        JavaInputDStream<ConsumerRecord<String, Measurement>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, consumerParams)
        );

        Producer<String, String> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                producer.close();
                ssc.close();
            }
        });

        stream.foreachRDD(rdd -> {
            JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(record -> {
                Measurement value = record.value();
                return new Tuple2<>(value.getDevice(), value.getValue());
            }).cache();

            JavaPairRDD<String, Integer> minPair = pairRDD.reduceByKey((a, b) -> a < b ? a : b);
            JavaPairRDD<String, Integer> maxPair = pairRDD.reduceByKey((a, b) -> a > b ? a : b);

            JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = pairRDD.mapValues(value -> new Tuple2(value, 1));
            JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount
                    .reduceByKey((tuple1, tuple2) -> new Tuple2(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
            JavaPairRDD<String, Integer> averagePair = reducedCount.mapToPair(getAverageByKey);

            JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Integer>> results = minPair.join(maxPair).join(averagePair);
            List<String> values = results.map(getStatistics).collect();
            values.forEach(val -> producer.send(new ProducerRecord(OUTPUT, val)));
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer> getAverageByKey = (tuple) -> {
        Tuple2<Integer, Integer> val = tuple._2;
        int total = val._1;
        int count = val._2;
        Tuple2<String, Integer> averagePair = new Tuple2(tuple._1, total / count);
        return averagePair;
    };

    private static Function<Tuple2<String, Tuple2<Tuple2<Integer, Integer>, Integer>>, String> getStatistics = result -> {
        int min = result._2._1._1;
        int max = result._2._1._2;
        int avg = result._2._2;
        String device = result._1;
        return String.format("device: %s, avg: %d, min: %d, max: %d", device, avg, min, max);
    };
}

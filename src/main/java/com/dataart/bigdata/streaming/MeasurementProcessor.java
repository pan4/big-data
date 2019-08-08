package com.dataart.bigdata.streaming;

import com.dataart.bigdata.kafka.Measurement;
import com.dataart.bigdata.kafka.MeasurementDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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


public class MeasurementProcessor {

    public static void main(String[] args) {

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "group");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", MeasurementDeserializer.class);

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Measurement Processor");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        List<String> topics = Collections.singletonList("two-partitions");

        JavaInputDStream<ConsumerRecord<String, Measurement>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        stream.foreachRDD(rdd -> {
            JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(record -> {
                Measurement value = record.value();
                return new Tuple2<>(value.getDevice(), value.getValue());
            }).cache();

            List<Tuple2<String, Integer>> min = pairRDD.reduceByKey((a, b) -> a < b ? a : b).collect();

            List<Tuple2<String, Integer>> max = pairRDD.reduceByKey((a, b) -> a > b ? a : b).collect();
//
            //count each values per key
            JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = pairRDD.mapValues(value -> new Tuple2(value, 1));
            //add values by reduceByKey
            JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount
                    .reduceByKey((tuple1, tuple2) -> new Tuple2(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
            //calculate average
            JavaPairRDD<String, Integer> averagePair = reducedCount.mapToPair(getAverageByKey);

            System.out.println(min);
            System.out.println(max);
            averagePair.foreach(data -> {
                System.out.println("Key="+data._1() + " Average=" + data._2());
            });
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

}

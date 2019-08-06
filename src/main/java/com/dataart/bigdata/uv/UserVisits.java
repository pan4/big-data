package com.dataart.bigdata.uv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

import static com.dataart.bigdata.uv.UserVisitsSchema.COUNTRY_CODE;

public class UserVisits {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UserVisits").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> stringJavaRDD = sc.textFile("/src/main/resources/uservisits");
        JavaRDD<String> stringJavaRDD = sc.textFile("src\\main\\resources\\uservisits");

        JavaRDD<String[]> splitRecords = stringJavaRDD.map(s -> s.split(","));

        List<Tuple2<Integer, String>> top10 = splitRecords
                .mapToPair(arr -> new Tuple2<>(arr[COUNTRY_CODE.ordinal()], 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(10);

        System.out.println(top10);
    }
}

package com.dataart.bigdata.bu;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

import static com.dataart.bigdata.uv.UserVisitsSchema.COUNTRY_CODE;

public class BrowserUsage {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BrowserUsage").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.set("fs.s3a.access.key", "AKIAXOSA343UTGYTTA7F");
        hadoopConf.set("fs.s3a.secret.key", "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");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");


        JavaRDD<String> stringJavaRDD = sc.textFile("s3a://big-data-benchmark/pavlo/text/tiny/uservisits");

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

package com.dataart.bigdata.bu;

import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.dataart.bigdata.uv.UserVisitsSchema.*;

public class BrowserUsage {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Browser Usage")
                .master("local[*]")
                .getOrCreate();

        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.access.key", "AKIAXOSA343UTGYTTA7F");
        hadoopConf.set("fs.s3a.secret.key", "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");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");


        UserAgentAnalyzer uaa = UserAgentAnalyzer
                .newBuilder()
                .hideMatcherLoadStats()
                .withCache(10000)
                .build();

        Dataset<Row> blocks = spark.read()
                .format("csv")
                .option("header", "true")
                .load("/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Blocks.csv")
                .persist();

        Dataset<Row> locations = spark.read()
                .format("csv")
                .option("header", "true")
                .load("/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Location.csv")
                .persist();

//        blocks.printSchema();
//        locations.printSchema();
//        locations.show(10);

//        long ip = 16777471;
//        long ip = 10;
//        Dataset<Row> where = blocks.select("locId").where(blocks.col("startIpNum").leq(ip)
//                .and(blocks.col("endIpNum").geq(ip)));
//
//        System.out.println(where.takeAsList(1).isEmpty());

//        System.out.println(blocks == null);
//        List<Row> locId = blocks.select("locId").where(blocks.col("startIpNum").leq(ip)
//                .and(blocks.col("endIpNum").geq(ip)))
//                .takeAsList(1);
//        System.out.println(locId.isEmpty());

        JavaRDD<BrowserStats> javaRDD = spark.read()
                .textFile("s3a://big-data-benchmark/pavlo/text/tiny/uservisits/part-00000")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    BrowserStats stats = new BrowserStats();
                    stats.setAgentName(uaa.parse(parts[USER_AGENT.ordinal()]).getValue(UserAgent.AGENT_NAME));
                    stats.setVisitDate(parts[VISIT_DATE.ordinal()]);

//                    long ip = 10;
//                    System.out.println(blocks == null);
//                    List<Row> locId = blocks.select("locId").where(blocks.col("startIpNum").leq(ip)
//                            .and(blocks.col("endIpNum").geq(ip)))
//                            .takeAsList(1);
//                    if (locId.isEmpty()) {
//                        return stats;
//                    }
                    List<Row> city = locations.select("city")
                            .where(locations.col("locId").equalTo(255027))
                            .takeAsList(1);
                    if (!city.isEmpty()) {
                        stats.setCity(city.get(0).getString(0));
                    }
                    return stats;
                });
//
        System.out.println(javaRDD.take(10));
//
//        Dataset<Row> browserDF = spark.createDataFrame(javaRDD, BrowserStats.class);
//
//        browserDF.groupBy(browserDF.col("agentName"),
//                year(browserDF.col("visitDate")).as("year"),
//                month(browserDF.col("visitDate")).as("month"))
//                .count()
//                .orderBy("year", "month").show();
    }

    private static long ipToLong(String ipAddress) {

        String[] ipAddressInArray = ipAddress.split("\\.");

        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {

            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);

        }

        return result;
    }
}

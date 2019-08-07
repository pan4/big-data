package com.dataart.bigdata.bu;

import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.dataart.bigdata.uv.UserVisitsSchema.SOURCE_IP;
import static com.dataart.bigdata.uv.UserVisitsSchema.USER_AGENT;
import static com.dataart.bigdata.uv.UserVisitsSchema.VISIT_DATE;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;

public class BrowserUsage {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Browser Usage")
//                .master("local[*]")
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
                .load("/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Blocks.csv");

        Dataset<Row> locations = spark.read()
                .format("csv")
                .option("header", "true")
                .load("/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Location.csv");

        JavaRDD<BrowserStats> javaRDD = spark.read()
                .textFile("s3a://big-data-benchmark/pavlo/text/tiny/uservisits/part-00000")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    BrowserStats stats = new BrowserStats();
                    stats.setAgentName(uaa.parse(parts[USER_AGENT.ordinal()]).getValue(UserAgent.AGENT_NAME));
                    stats.setVisitDate(parts[VISIT_DATE.ordinal()]);
                    stats.setNumIp(ipToLong(parts[SOURCE_IP.ordinal()]));
                    return stats;
                });

        Dataset<Row> visits = spark.createDataFrame(javaRDD, BrowserStats.class);

        Dataset<Row> withLocId = visits.join(blocks, blocks.col("startIpNum").leq(visits.col("numIp"))
                .and(blocks.col("endIpNum").geq(visits.col("numIp"))),"left")
                .select("agentName", "visitDate", "locId");

        Dataset<Row> withCity = withLocId.join(locations, withLocId.col("locId").equalTo(locations.col("locId")),"left")
                .select("agentName", "visitDate", "city");

        withCity.groupBy(withCity.col("agentName"), withCity.col("city"),
                year(withCity.col("visitDate")).as("year"),
                month(withCity.col("visitDate")).as("month"))
                .count()
                .orderBy("year", "month")
                .show();
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

package com.dataart.bigdata.ml;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class TagRecommender {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Tag Recommender")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "row")
//                .load("C:\\Users\\apanchenko\\Projects\\big-data\\stackoverflow.com-Posts\\Posts.xml");
                .load("src\\main\\resources\\Posts.xml");

//        Encoder<Tag> encoder = Encoders.bean(Tag.class);
//        Dataset<Tag> tagDataset = df.select("_Id", "_Tags").flatMap((Row row) -> {
//            String[] tags = row.getString(1).split(">");
//            List<Tag> list = new ArrayList<>();
//            for (String name : tags) {
//                name = name.substring(1);
//                Tag tag = new Tag();
//                tag.setId(row.getLong(0));
//                tag.setName(name);
//                list.add(tag);
//            }
//            return list.iterator();
//        }, encoder);
//
//        ALS.trainImplicit()

//        System.out.println(df.count());
        df.printSchema();
    }
}

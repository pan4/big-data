package com.dataart.bigdata.ml;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class ModelTrainer {
    public static final String MODEL_PATH = "target/tmp/myCollaborativeFilter";
    public static final String INT_TO_TAG_PATH = "target/tmp/tagNames.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("PostTags Recommender")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "row")
//                .load("C:\\Users\\apanchenko\\Projects\\big-data\\stackoverflow.com-Posts\\Posts.xml");
                .load("src\\main\\resources\\Posts.xml");

        Encoder<PostTags> postTagEncoder = Encoders.bean(PostTags.class);
        Dataset<PostTags> tagDataset = df.select("_Id", "_Tags").flatMap((Row row) -> {
            List<PostTags> list = new ArrayList<>();
            if (row.getString(1) == null) {
                return list.iterator();
            }
            String[] tags = row.getString(1).split(">");
            for (String name : tags) {
                name = name.substring(1);
                int postId = (int) row.getLong(0);
                PostTags postTags = new PostTags(postId, name);
                list.add(postTags);
            }
            return list.iterator();
        }, postTagEncoder);

        tagDataset.select("tagName")
                .distinct()
                .createOrReplaceTempView("df");
        Dataset<Row> tagNames = spark.sql("select row_number() over (order by 'tagName') as tagId, * from df");
        tagNames.write()
                .csv(INT_TO_TAG_PATH);

        Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);
        Dataset<Rating> rating = tagDataset.join(tagNames, tagDataset.col("tagName").equalTo(tagNames.col("tagName")))
                .select("postId", "tagId")
                .map((Row row) -> new Rating(row.getInt(0), row.getInt(1), (double) 1), ratingEncoder);

        MatrixFactorizationModel model = ALS.trainImplicit(rating.rdd(), 30, 10);
        model.save(spark.sparkContext(), MODEL_PATH);
    }
}

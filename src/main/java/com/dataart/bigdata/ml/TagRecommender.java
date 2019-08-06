package com.dataart.bigdata.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static com.dataart.bigdata.ml.ModelTrainer.INT_TO_TAG_PATH;
import static com.dataart.bigdata.ml.ModelTrainer.MODEL_PATH;

public class TagRecommender {
    private static JavaSparkContext sc;
    private static JavaRDD<String> tagNames;
    private static MatrixFactorizationModel model;

    public static void main(String[] args) {
        System.out.println(args);

        SparkConf conf = new SparkConf().setAppName("Tag Recommender").setMaster("local[*]");
        sc = new JavaSparkContext(conf);

        tagNames = sc.textFile(INT_TO_TAG_PATH);
        model = MatrixFactorizationModel.load(sc.sc(), MODEL_PATH);

        System.out.println(recommend(4, 5));
    }

    private static List<Tuple2<Double, String>> recommend(int postId, int number) {
        List<Rating> ratings = Arrays.asList(model.recommendProducts(postId, number));

        JavaPairRDD<Integer, Double> idRating = sc.parallelize(ratings).mapToPair(rating -> new Tuple2<>(rating.product(), rating.rating()));

        JavaPairRDD<Integer, String> idName = tagNames.mapToPair(line -> {
            String[] split = line.split(",");
            return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
        });

        return idRating.join(idName).values().collect();
    }
}

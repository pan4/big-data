package com.dataart.bigdata.as;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.explode;

public class AverageSalary {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Average Salary")
//                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("/home/alex/Projects/apanchenko-bgd02/src/main/resources/hh-vacs");

        Dataset<Row> sp = df.select(df.col("address.city"),
                df.col("salary.from"),
                df.col("salary.to"),
                explode(df.col("specializations")).as("spec"));

        Dataset<Row> spName = sp.select(sp.col("city"),
                sp.col("from"),
                sp.col("to"),
                sp.col("spec").getField("name").as("specName"));

        spName.select(spName.col("city"),
                spName.col("specName"),
                spName.col("from").plus(spName.col("to")).divide(2).as("sal"))
                .where(spName.col("specName").contains("Программирование, Разработка")
                        .or(spName.col("specName").contains("Системный администратор"))
                        .or(spName.col("specName").contains("Дизайнер")))
                .groupBy(spName.col("city"), spName.col("specName"))
                .avg("sal")
                .orderBy(spName.col("specName"))
                .show();
    }
}

package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class PivotAggregation {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score")),2).alias("avg score"),
                                                                    round(stddev(col("score")),2).alias("stddev"));
        dataset.show();

        spark.close();
    }
}

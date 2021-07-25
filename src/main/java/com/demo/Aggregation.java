package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static  org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class Aggregation {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();

        /*
        Alternate - 1 does not word
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset = dataset.groupBy("subject").max("score"); //does not work
         */



        /*
        //Alternate - 1
        Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/students.csv");
        dataset = dataset.groupBy("subject").max("score");
        */
        /*
        //Alternate - 2 - not supported
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset = dataset.groupBy("subject").max(col("score").cast(DataTypes.IntegerType));
        */

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score"),
                                                      min(col("score").cast(DataTypes.IntegerType)).alias("min score"));

        dataset.show();
        spark.close();
    }
}

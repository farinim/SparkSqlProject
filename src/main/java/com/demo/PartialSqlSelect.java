package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.first;

public class PartialSqlSelect {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        //Use Sql expession based select
        Dataset<Row> result1 = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
        result1.show(100);

        //use select + functions static apis
        Dataset<Row> result2 = dataset.select(col("level"),
                                              date_format(col("datetime"), "MMMM").alias("month"),
                                              date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        result2 = result2.groupBy(col("level"),col("month"),col("monthnum")).count();
        result2 = result2.orderBy("monthnum","level");
        result2 = result2.drop(col("monthnum"));
        result2.show(100);

        spark.close();
    }
}

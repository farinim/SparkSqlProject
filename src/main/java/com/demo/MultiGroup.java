package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MultiGroup {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                " from logging_table group by level,month");
        result.show();

        result.createOrReplaceTempView("result_table");

        Dataset<Row> total = spark.sql("select sum(total) from result_table");
        total.show();

        spark.close();
    }
}

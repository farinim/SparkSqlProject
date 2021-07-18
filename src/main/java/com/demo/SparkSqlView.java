package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlView {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset.createOrReplaceTempView("student_table");

        Dataset<Row> result1 = spark.sql("select score,year from student_table where subject='French'");
        result1.show();

        Dataset<Row> result2 = spark.sql("select distinct(year) from student_table where subject='French' order by year desc");
        result2.show();
        spark.close();
    }
}

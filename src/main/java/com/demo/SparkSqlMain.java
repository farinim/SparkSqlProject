package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;

public class SparkSqlMain {

    public static void main(String[] args){

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                                                   .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                                                   .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        dataset.show();

        //System.out.println("Number of rows = " + dataset.count());
        Row first = dataset.first();
        String subject = first.get(2).toString();
        System.out.println("Subject : " + subject);
        System.out.println("Score : " + first.getAs("score"));

        //Filters
        Dataset<Row> mordernArtResultPlain = dataset.filter("subject='Modern Art' and year > 2008");
        mordernArtResultPlain.show();

        //filter using lambda
        Dataset<Row> mordernArtResultWithLambda = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
                && Integer.parseInt(row.getAs("year")) > 2008);
        mordernArtResultWithLambda.show();

        //filter using Spark SQL Column
        Column subjectCol = dataset.col("subject");
        Column yearCol = dataset.col("year");
        Dataset<Row> mordernArtResultUsingColumn = dataset.filter(subjectCol.equalTo("Modern Art").and(yearCol.gt(2008)));
        mordernArtResultUsingColumn.show();

        //filter using DSL - functions col static method
        Dataset<Row> mordernArtResultUsingColFn = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").gt(2008)));
        mordernArtResultUsingColFn.show();

        spark.close();

    }
}

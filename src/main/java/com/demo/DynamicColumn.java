package com.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class DynamicColumn {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/Work/tmp")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students.csv");
        //Alternate 1 - Inline function
        Dataset<Row> result1 = dataset.withColumn("pass", lit("YES"));
        //Dataset<Row> result1 = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        //result1.show();

        //Alternate 2 - UDF + Lambda function
        spark.udf().register("hasPassed",(String grade) -> grade.equals("A+"), DataTypes.BooleanType);
        Dataset<Row> result2 = dataset.withColumn("pass", callUDF("hasPassed", col("grade")));
        //result2.show();

        //Alternate 3 - UDF + multi-line lambda
        spark.udf().register("examResult", (String subject, String grade) -> {
            if (subject.equals("Biology")) {
                return grade.equals("A");
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);
        Dataset<Row> result3 = dataset.withColumn("pass", callUDF("examResult", col("subject"),col("grade")));
        result3.show();

        spark.close();
    }
}

package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import java.sql.SQLException;
import org.apache.spark.sql.*;


class ReadCSV {
    public static void main(String csv[]) throws SQLException {
//tema1
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("C:\\Users\\Acer\\Desktop\\SEM II AN II\\Big Data Project\\Big_Data\\src\\main\\resources\\Erasmus.csv");
//tema 2

        dataset.printSchema();
        dataset = dataset.filter(functions.col("Receiving Country Code").isin("ES", "FR", "EL"));
        dataset.select("Receiving Country Code", "Sending Country Code").show(50, false);
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count().withColumnRenamed("count", "Number of students")
                .orderBy(functions.col("Receiving Country Code").desc())
                .show(100);
//tema 3
        dataBase(dataset, "ES", "Estonia");
        dataBase(dataset, "FR", "Franta");
        dataBase(dataset, "EL", "Elvetia");
        dataBase(dataset, "RO", "Romania");
    }


    public static void dataBase(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/erasmus?serverTimezone=UTC")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "Mariapatricia18")
                .save(tableName + ".erasmus");
    }
}
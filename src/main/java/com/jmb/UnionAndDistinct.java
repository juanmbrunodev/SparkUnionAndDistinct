package com.jmb;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnionAndDistinct {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnionAndDistinct.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    private static final String PATH_RESOURCES_DF1 = "src/main/resources/spark-data/us_salesmen.csv";
    private static final String PATH_RESOURCES_DF2 = "src/main/resources/spark-data/uk_salesmen.csv";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        UnionAndDistinct app = new UnionAndDistinct();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {
        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("UnionAndDistinct")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a DataFrame
        Dataset<Row> dfSalesUs = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PATH_RESOURCES_DF1);

        Dataset<Row> dfSalesUk = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PATH_RESOURCES_DF2);

        //Print the 5 first records to inspect both the schema and data of the loaded DataFrames
        dfSalesUs.show();
        dfSalesUk.show();

    }

}

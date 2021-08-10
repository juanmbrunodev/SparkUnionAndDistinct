package com.jmb;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.lit;


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

        //Ingest data from CSV files into a DataFrames
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
        LOGGER.info("US Salesmen DataFrame Ingested: ");
        dfSalesUs.show();
        LOGGER.info("UK Salesmen DataFrame Ingested: ");
        dfSalesUk.show();

        //Inspect both dfs schemas to make sure the structure matches thus allowing a Union Transformation
        dfSalesUs.printSchema();
        dfSalesUk.printSchema();

        //Perform a Union of the DataFrames ingested
        Dataset<Row> allEmployees = dfSalesUs.union(dfSalesUk);

        //Print resulting merged DataFrame
        allEmployees.show();

        //Rearrange columns order.
        Dataset<Row> dfUkSalesReOrg = dfSalesUk.drop("department").withColumn("department", lit("TBD"));

        dfUkSalesReOrg.show(2);

        //Now attempt a unionByName - union based on matching columns regardless of the schemas
        //Perform a Union of the DataFrames ingested
        Dataset<Row> allEmployeesUnionByName = dfSalesUs.unionByName(dfUkSalesReOrg);

        // Print results
        allEmployeesUnionByName.show();

        //Drop duplicates based on employee name and department as identity criteria
        Dataset<Row> uniqueAllEmployees = allEmployees.dropDuplicates("employee_name", "department");

        //Print results
        uniqueAllEmployees.show();

    }

}

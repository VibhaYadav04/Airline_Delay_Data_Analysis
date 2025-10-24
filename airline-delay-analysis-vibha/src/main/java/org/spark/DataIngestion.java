package org.spark;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.exception.GlobalExceptionHandler;
import org.utils.Config;
import org.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataIngestion {
    private static final Logger logger = LoggerFactory.getLogger(DataIngestion.class);

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        try {
            String layer = Constants.BRONZE_SCHEMA;  // ingestion always starts from bronze

            // ---------------- Input CSVs ----------------
            String airlineCsv = Config.get(Constants.INPUT_PATH) + "Airline_Delay_Cause.csv";
            String carrierCsv = Config.get(Constants.INPUT_PATH) + "carrier_lookup.csv";
            String airportCsv = Config.get(Constants.INPUT_PATH) + "airports.csv";

            // ---------------- Output Path ----------------
            String outputPath = Config.getOutputPath(layer);

            // ---------------- Load Datasets ----------------
            Dataset<Row> rawData = loadCsv(spark, airlineCsv);
            Dataset<Row> carrierLookup = loadCsv(spark, carrierCsv);
            Dataset<Row> airportLookup = loadCsv(spark, airportCsv);

            // Task 2.2 - Validate schema
            validateSchema(rawData);

            // ---------------- Save datasets ----------------
            save(rawData, outputPath + "raw_dataset", "raw_dataset", layer);
            save(carrierLookup, outputPath + "carrier_lookup", "carrier_lookup", layer);
            save(airportLookup, outputPath + "airport_lookup", "airport_lookup", layer);

            logger.info("Data ingestion completed. Bronze layer stored at {} and database {}",
                    outputPath, Config.getSchema(layer));

        } catch (Exception e) {
            GlobalExceptionHandler.handle(e, "Ingestion failed");
        } finally {
            spark.stop();
        }
    }

    // Create Spark Session
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Airline Data Ingestion")
                .master("local[*]")
                .getOrCreate();
    }

    // Task 2.1 - Load CSV
    private static Dataset<Row> loadCsv(SparkSession spark, String path) {
        logger.info("Loading CSV file: {}", path);
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    // Task 2.2 - Validate Schema
    private static void validateSchema(Dataset<Row> df) {
        logger.info("Validating numeric column schema...");
        df.select("arr_delay", "carrier_delay", "weather_delay",
                        "nas_delay", "security_delay", "late_aircraft_delay")
                .printSchema();
    }

    // Save Data as Parquet + Database
    private static void save(Dataset<Row> df, String parquetPath, String tableName, String layer) {
        try {
            logger.info("Saving parquet: {}", parquetPath);
            df.coalesce(1)
                    .write()
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save(parquetPath);

            logger.info("Overwriting data to table {}.{}", Config.getSchema(layer), tableName);
            df.write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", Config.getDbUrl(layer))
                    .option("dbtable", tableName)
                    .option("user", Config.getDbUser())
                    .option("password", Config.getDbPassword())
                    .option("driver", Config.getDbDriver())
                    .save();

        } catch (Exception e) {
            GlobalExceptionHandler.handle(e, "Failed to save dataset " + tableName);
        }
    }
}

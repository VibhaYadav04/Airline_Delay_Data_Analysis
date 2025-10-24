package org.spark;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import org.exception.GlobalExceptionHandler;
import org.utils.Config;
import org.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCleaning {
    private static final Logger logger = LoggerFactory.getLogger(DataCleaning.class);

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        try {
            String bronzeLayer = Constants.BRONZE_SCHEMA;
            String silverLayer = Constants.SILVER_SCHEMA;

            String silverPath = Config.getOutputPath(silverLayer);

            // ---------------- Raw Dataset ----------------
            Dataset<Row> df = loadFromTable(spark, bronzeLayer, "raw_dataset");

            // Task 3.1  Remove duplicates
            df = distinct(df);
            // Task 3.1  Handle missing values
            df = handleMissingValues(df);
            // Task 3.2  Convert year & month into proper date
            df = convertDate(df);
            // Task 3.3  Clean categorical fields
            df = cleanCategoricalFields(df);
            // Task 3.4  Filter corrupted rows
            df = filterCorruptedRows(df);

            save(df, silverPath + "cleaned_dataset", "cleaned_dataset", silverLayer);
            logger.info("Cleaned dataset stored in silver layer. Row count = {}", df.count());

            // ---------------- Carrier Lookup ----------------
            Dataset<Row> carrierLookup = loadFromTable(spark, bronzeLayer, "carrier_lookup");
            carrierLookup = cleanAllStringColumns(carrierLookup);
            save(carrierLookup, silverPath + "cleaned_carrier_lookup", "cleaned_carrier_lookup", silverLayer);

            // ---------------- Airport Lookup ----------------
            Dataset<Row> airportLookup = loadFromTable(spark, bronzeLayer, "airport_lookup");
            airportLookup = cleanAllStringColumns(airportLookup);
            save(airportLookup, silverPath + "cleaned_airport_lookup", "cleaned_airport_lookup", silverLayer);

        } catch (Exception e) {
            GlobalExceptionHandler.handle(e, "Data cleaning failed");
        } finally {
            spark.stop();
        }
    }

    // Spark session
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Airline Delay Data Cleaning")
                .master("local[*]")
                .getOrCreate();
    }

    // Load data from a DB table
    private static Dataset<Row> loadFromTable(SparkSession spark, String layer, String tableName) {
        logger.info("Loading from DB Table: {}.{}", Config.getSchema(layer), tableName);
        return spark.read()
                .format("jdbc")
                .option("url", Config.getDbUrl(layer))
                .option("dbtable", tableName)
                .option("user", Config.getDbUser())
                .option("password", Config.getDbPassword())
                .option("driver", Config.getDbDriver())
                .load();
    }

    // Drop duplicate
    private static Dataset<Row> distinct(Dataset<Row> df) {
        return df.dropDuplicates();
    }

    // Task 3.1 - Handle missing values
    private static Dataset<Row> handleMissingValues(Dataset<Row> df) {
        return df.withColumn("arr_delay", col("arr_delay").cast("double"))
                .withColumn("arr_flights", col("arr_flights").cast("int"))
                .filter(col("arr_flights").isNotNull());
    }

    // Task 3.2 - Convert year & month into proper date
    private static Dataset<Row> convertDate(Dataset<Row> df) {
        return df.withColumn(
                "date",
                to_date(concat(col("year"), lit("-"), lpad(col("month"), 2, "0"), lit("-01")), "yyyy-MM-dd")
        );
    }

    // Task 3.3 - Clean categorical fields
    private static Dataset<Row> cleanCategoricalFields(Dataset<Row> df) {
        return df.withColumn("carrier", upper(trim(col("carrier"))))
                .withColumn("carrier_name", upper(trim(col("carrier_name"))))
                .withColumn("airport_name", upper(trim(col("airport_name"))));
    }

    // Task 3.4 - Filter corrupted rows
    private static Dataset<Row> filterCorruptedRows(Dataset<Row> df) {
        return df.filter(col("arr_flights").geq(0))
                .filter(col("arr_delay").geq(0));
    }

    // Clean ALL string columns → uppercase + trim
    private static Dataset<Row> cleanAllStringColumns(Dataset<Row> df) {
        for (String colName : df.columns()) {
            if (df.schema().apply(colName).dataType().simpleString().equals("string")) {
                df = df.withColumn(colName, upper(trim(col(colName))));
            }
        }
        return df;
    }

    // Save both parquet and DB
    private static void save(Dataset<Row> df, String parquetPath, String tableName, String layer) {
        try {
            logger.info("Saving parquet: {}", parquetPath);
            df.coalesce(1)
                    .write()
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .save(parquetPath);

            logger.info("Overwriting DB Table: {}.{}", Config.getSchema(layer), tableName);
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

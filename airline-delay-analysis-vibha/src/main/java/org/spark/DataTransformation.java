package org.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.exception.GlobalExceptionHandler;
import org.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.utils.Constants;

public class DataTransformation {
    private static final Logger logger = LoggerFactory.getLogger(DataTransformation.class);

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        try {
            String silverSchema = Config.getSchema(Constants.SILVER_SCHEMA);
            String goldPath     = Config.getOutputPath(Constants.GOLD_SCHEMA);

            // ---------------- Load Silver Tables ----------------
            Dataset<Row> airlineDf = loadTable(spark, silverSchema + ".cleaned_dataset", "silver.cleaned_dataset");
            Dataset<Row> carrierDf = loadTable(spark, silverSchema + ".cleaned_carrier_lookup", "silver.cleaned_carrier_lookup");
            Dataset<Row> airportDf = loadTable(spark, silverSchema + ".cleaned_airport_lookup", "silver.cleaned_airport_lookup");

            // Task 4.1 and 4.2 - Derived Fields
            Dataset<Row> df = addDerivedFields(airlineDf);
            logger.info("Added on_time_flag and total_delay columns. Row count = {}", df.count());

            // Task 4.3 and 4.4 - Normalize with Lookups
            Dataset<Row> aggregatedDf = normalizedDf(df, carrierDf, airportDf);

            // Task 5.2 - Build Dimensions from aggregated dataset
            Dataset<Row> dimCarrier = aggregatedDf.select("carrier", "carrier_name").distinct();
            Dataset<Row> dimAirport = aggregatedDf.select("airport", "airport_name").distinct();

            // Task 5.5 Populate Dimension Tables
            saveAsParquet(dimCarrier, goldPath + "/dim_carrier");
            saveToDb(dimCarrier, "dim_carrier");

            saveAsParquet(dimAirport, goldPath + "/dim_airport");
            saveToDb(dimAirport, "dim_airport");

            // Reload Dimensions with IDs from DB
            Dataset<Row> dimCarrierDb = loadTable(spark, "dim_carrier", "gold.dim_carrier");
            Dataset<Row> dimAirportDb = loadTable(spark, "dim_airport", "gold.dim_airport");

            // Task 5.1 - Build Fact Table
            Dataset<Row> factDf = aggregatedDf
                    .join(dimCarrierDb, "carrier")
                    .join(dimAirportDb, "airport")
                    .select(
                            col("year"),
                            col("month"),
                            col("airport_id"),
                            col("carrier_id"),
                            col("arr_flights"),
                            col("arr_delay"),
                            col("arr_del15"),
                            col("carrier_ct"),
                            col("weather_ct"),
                            col("nas_ct"),
                            col("security_ct"),
                            col("late_aircraft_ct"),
                            col("carrier_delay"),
                            col("weather_delay"),
                            col("nas_delay"),
                            col("security_delay"),
                            col("late_aircraft_delay"),
                            col("arr_cancelled"),
                            col("arr_diverted"),
                            col("on_time_flag"),
                            col("total_delay"),
                            col("date")
                    );

            // Task 5.4 - Populate Fact Table
            saveAsParquet(factDf, goldPath + "/fact_flight_performance");
            saveToDb(factDf, "fact_flight_performance");

            logger.info("Fact table written successfully");

        } catch (Exception e) {
            GlobalExceptionHandler.handle(e, "Transformation failed");
        } finally{
            spark.stop();
        }
    }

    // Create Spark Session
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Airline Delay Data Transformation")
                .master("local[*]")
                .getOrCreate();
    }

    // Load table from database
    private static Dataset<Row> loadTable(SparkSession spark, String table, String alias) {
        logger.info("Loading table: {}", alias);
        return spark.read()
                .format("jdbc")
                .option("url", Config.getDbUrl("gold"))
                .option("dbtable", table)
                .option("user", Config.getDbUser())
                .option("password", Config.getDbPassword())
                .option("driver", Config.getDbDriver())
                .load();
    }

    // Save to Parquet
    private static void saveAsParquet(Dataset<Row> df, String outputPath) {
        df.coalesce(1)
                .write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save(outputPath);
    }

    // Save to database
    private static void saveToDb(Dataset<Row> df, String tableName) {
        df.write()
                .format("jdbc")
                .option("url", Config.getDbUrl("gold"))
                .option("dbtable", tableName)
                .option("user", Config.getDbUser())
                .option("password", Config.getDbPassword())
                .option("driver", Config.getDbDriver())
                .mode(SaveMode.Append)
                .save();
        logger.info("Table {} written to DB", tableName);
    }

    // Task 4.1 and 4.2 - Derived Fields Helper
    public static Dataset<Row> addDerivedFields(Dataset<Row> airlineDf) {
        return airlineDf
                .withColumn("on_time_flag", when(col("arr_delay").lt(15), lit(1)).otherwise(lit(0)))
                .withColumn("total_delay",
                        coalesce(col("carrier_delay"), lit(0))
                                .plus(coalesce(col("weather_delay"), lit(0)))
                                .plus(coalesce(col("nas_delay"), lit(0)))
                                .plus(coalesce(col("security_delay"), lit(0)))
                                .plus(coalesce(col("late_aircraft_delay"), lit(0))));
    }

    // Task 4.3 and 4.4 - Normalization Helper
    public static Dataset<Row> normalizedDf(Dataset<Row> df,
                                            Dataset<Row> carrierRef,
                                            Dataset<Row> airportRef) {
        Dataset<Row> carrierRenamed = carrierRef
                .withColumnRenamed("carrier", "carrier_ref")
                .withColumnRenamed("carrier_name", "carrier_name_ref");

        Dataset<Row> airportRenamed = airportRef
                .withColumnRenamed("iata_code", "airport_ref")
                .withColumnRenamed("name", "airport_name_ref");

        return df
                .join(carrierRenamed.dropDuplicates(),
                        df.col("carrier").equalTo(carrierRenamed.col("carrier_ref")),
                        "left")
                .join(airportRenamed.dropDuplicates(),
                        df.col("airport").equalTo(airportRenamed.col("airport_ref")),
                        "left")
                .withColumn("carrier_name", coalesce(col("carrier_name_ref"), col("carrier_name")))
                .withColumn("airport_name", coalesce(col("airport_name_ref"), col("airport_name")))
                .drop("carrier_ref", "carrier_name_ref", "airport_ref", "airport_name_ref");
    }
}

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Import utility functions
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.load_latest_data import load_latest_data
from utils.save_kpi_data import save_kpi_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def main():
    """
    Main function to calculate the top 3 aircraft models by airline country.
    """
    try:
        # Step 1: Initialize Spark session
        logging.info("\033[1;34mInitializing Spark session...\033[0m")
        spark = SparkSession.builder.appName("Top3AircraftModelsByCountry").getOrCreate()

        # Base paths for loading and saving data
        base_extracted_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ingested_data"))
        results_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/results"))

        logging.info("\033[1;34mStarting process to find the top 3 aircraft models by airline country...\033[0m")

        # Step 2: Load flight and airline data
        logging.info("\033[1;32mLoading flight and airline data...\033[0m")
        df_flights = load_latest_data(spark, base_extracted_path, "flights")
        df_airlines = load_latest_data(spark, base_extracted_path, "airlines")

        # Step 3: Join flights with airline data to get country information
        logging.info("\033[1;32mJoining flights with airline data to get country information...\033[0m")
        df_flights = df_flights.join(
            df_airlines.select(
                col("ICAO").alias("airlineCode"),
                col("Country").alias("airlineCountry")
            ),
            on="airlineCode",
            how="left"
        )

        # Step 4: Group by country and aircraft model, and rank the models by usage
        logging.info("\033[1;34mCalculating top 3 aircraft models by airline country...\033[0m")
        top_3_aircraft_models = (
            df_flights.groupBy("airlineCountry", "aircraft")
            .agg(count("*").alias("usage_count"))  # Count flights for each aircraft model
            .withColumn(
                "rank",
                row_number().over(
                    Window.partitionBy("airlineCountry").orderBy(col("usage_count").desc())
                )
            )
            .filter(col("rank") <= 3)  # Keep only the top 3 models per country
            .select("airlineCountry", "aircraft", "usage_count", "rank")  # Select relevant columns
        )

        # Step 5: Save the result
        logging.info("\033[1;32mSaving the result to disk...\033[0m")
        save_kpi_data(
            df=top_3_aircraft_models,
            base_path=results_path,
            folder_name="top_3_aircraft_models_by_country",
            description="Saving the top 3 aircraft models by airline country..."
        )

        logging.info("\033[1;32mTop 3 aircraft models by airline country successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error during processing: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

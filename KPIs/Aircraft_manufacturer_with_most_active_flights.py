import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count
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
    Main function to find the aircraft manufacturer with the most active flights.
    """
    try:
        # Step 1: Initialize Spark session
        logging.info("\033[1;34mInitializing Spark session...\033[0m")
        spark = SparkSession.builder.appName("Q5_AircraftManufacturerWithMostActiveFlights").getOrCreate()

        # Base paths for loading and saving data
        base_extracted_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ingested_data"))
        results_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/results"))

        logging.info("\033[1;34mStarting Q5: Aircraft manufacturer with the most active flights...\033[0m")

        # Step 2: Load flight data
        logging.info("\033[1;32mLoading flight data...\033[0m")
        df_flights = load_latest_data(spark, base_extracted_path, "flights")

        # Step 3: Filter ongoing flights
        logging.info("\033[1;32mFiltering ongoing flights...\033[0m")
        active_flights = df_flights.filter(col("flightStatus") == "0")

        # Step 4: Aggregate and rank flights by aircraft manufacturer
        logging.info("\033[1;34mCalculating active flight counts by manufacturer...\033[0m")
        top_manufacturer = (
            active_flights.groupBy("aircraft")  # Group by aircraft (manufacturer)
            .agg(count("*").alias("active_flight_count"))  # Count active flights
            .withColumn(  # Rank manufacturers by active flight count
                "rank",
                row_number().over(Window.orderBy(col("active_flight_count").desc()))
            )
            .filter(col("rank") == 1)  # Keep only the top manufacturer
            .drop("rank")  # Drop the rank column
        )

        # Step 5: Select necessary columns
        logging.info("\033[1;32mSelecting required columns...\033[0m")
        top_manufacturer = top_manufacturer.select("aircraft", "active_flight_count")

        # Step 6: Save the result
        logging.info("\033[1;34mSaving the result to disk...\033[0m")
        save_kpi_data(
            df=top_manufacturer,
            base_path=results_path,
            folder_name="aircraft_manufacturer_with_most_active_flights",
            description="Saving the aircraft manufacturer with the most active flights..."
        )

        logging.info("\033[1;32mQ5: Aircraft manufacturer with the most active flights successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error during processing: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

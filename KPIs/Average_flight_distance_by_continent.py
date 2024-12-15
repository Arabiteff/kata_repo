import os
import sys
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, isnan

# Import utility functions
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.load_latest_data import load_latest_data
from utils.save_kpi_data import save_kpi_data
from utils.calculate_distance import calculate_distance
from utils.map_zone import map_zone
from utils.repetitf_joins import join_flights_with_airports

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Base paths for loading and saving data
BASE_EXTRACTED_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ingested_data"))
RESULTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/results"))

def main():
    """
    Main function to calculate the average flight distance by continent.
    """
    try:
        # Step 1: Initialize Spark session
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder.appName("Q4_AverageFlightDistanceByContinent").getOrCreate()

        logging.info("\033[1;34mStarting Q4: Average flight distance by continent...\033[0m")

        # Step 2: Load data
        logging.info("\033[1;32mLoading data: flights, airports, and zones...\033[0m")
        df_flights = load_latest_data(spark, BASE_EXTRACTED_PATH, "flights")
        df_airports = load_latest_data(spark, BASE_EXTRACTED_PATH, "airports")
        df_zones = load_latest_data(spark, BASE_EXTRACTED_PATH, "zones")

        # Step 3: Join flights with airports for origin and destination coordinates
        logging.info("\033[1;32mJoining flights with airports...\033[0m")
        df_flights = join_flights_with_airports(df_flights, df_airports)

        # Step 4: Convert to Pandas for distance calculation and zone mapping
        logging.info("\033[1;32mCalculating flight distances and mapping zones...\033[0m")
        flights_pd = df_flights.toPandas()
        zones_pd = df_zones.toPandas()

        # Calculate distances
        flights_pd["flightDistance"] = flights_pd.apply(calculate_distance, axis=1)

        # Map zones to flights
        flights_pd["originContinent"] = flights_pd.apply(
            lambda row: map_zone(row["airportLat_origin"], row["airportLong_origin"], zones_pd)
            if pd.notna(row["airportLat_origin"]) else None, axis=1
        )
        flights_pd["destinationContinent"] = flights_pd.apply(
            lambda row: map_zone(row["airportLat_destination"], row["airportLong_destination"], zones_pd)
            if pd.notna(row["airportLat_destination"]) else None, axis=1
        )

        # Step 5: Convert back to Spark DataFrame
        logging.info("\033[1;32mConverting back to Spark DataFrame and calculating averages...\033[0m")
        result = (
            spark.createDataFrame(flights_pd)
            .filter(col("originContinent").isNotNull())
            .filter(~isnan(col("flightDistance")))
            .groupBy("originContinent")
            .agg(avg("flightDistance").alias("average_flight_distance"))
        )

        # Step 6: Save the result
        logging.info("\033[1;34mSaving the result to disk...\033[0m")
        save_kpi_data(
            df=result,
            base_path=RESULTS_PATH,
            folder_name="average_flight_distance_by_continent",
            description="Saving Q4: Average flight distance by continent..."
        )

        logging.info("\033[1;32mQ4: Average flight distance by continent successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error during processing: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

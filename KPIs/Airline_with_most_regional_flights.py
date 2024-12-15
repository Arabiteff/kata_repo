import os
import sys
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

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
    Main function to calculate the airline with the most regional flights per continent.
    """
    try:
        # Step 1: Initialize Spark session
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder.appName("Q2_AirlineWithMostRegionalFlights").getOrCreate()

        logging.info("\033[1;34mStarting Q2: Airline with most regional flights per continent...\033[0m")

        # Step 2: Load data
        logging.info("\033[1;32mLoading data: flights, airports, airlines, and zones...\033[0m")
        df_flights = load_latest_data(spark, BASE_EXTRACTED_PATH, "flights")
        df_airports = load_latest_data(spark, BASE_EXTRACTED_PATH, "airports")
        df_airlines = load_latest_data(spark, BASE_EXTRACTED_PATH, "airlines")
        df_zones = load_latest_data(spark, BASE_EXTRACTED_PATH, "zones")

        # Step 3: Join flights with airports to get origin and destination coordinates
        logging.info("\033[1;32mJoining flights with airports to get latitudes and longitudes...\033[0m")
        df_flights = join_flights_with_airports(df_flights, df_airports)

        # Step 4: Convert to Pandas DataFrame for distance calculations and zone mapping
        logging.info("\033[1;32mConverting flights data to Pandas and calculating distances...\033[0m")
        flights_pd = df_flights.toPandas()
        zones_pd = df_zones.toPandas()

        # Calculate flight distances
        flights_pd["flightDistance"] = flights_pd.apply(calculate_distance, axis=1)

        # Map zones to flights
        logging.info("\033[1;32mMapping zones to origin and destination coordinates...\033[0m")
        flights_pd["originContinent"] = flights_pd.apply(
            lambda row: map_zone(row["airportLat_origin"], row["airportLong_origin"], zones_pd)
            if pd.notna(row["airportLat_origin"]) else None, axis=1
        )
        flights_pd["destinationContinent"] = flights_pd.apply(
            lambda row: map_zone(row["airportLat_destination"], row["airportLong_destination"], zones_pd)
            if pd.notna(row["airportLat_destination"]) else None, axis=1
        )

        # Convert back to Spark DataFrame
        logging.info("\033[1;32mConverting back to Spark DataFrame...\033[0m")
        df_flights_with_distance_and_zones = spark.createDataFrame(flights_pd)

        # Step 5: Add airline names to the DataFrame
        logging.info("\033[1;32mJoining flights with airline names...\033[0m")
        df_flights_with_distance_and_zones = df_flights_with_distance_and_zones.join(
            df_airlines.select(col("ICAO").alias("airlineCode"), col("Name").alias("airline_name")),
            on="airlineCode",
            how="left"
        )

        # Step 6: Count and rank regional flights by continent and airline
        logging.info("\033[1;34mCalculating and ranking regional flights...\033[0m")
        result = (
            df_flights_with_distance_and_zones
            .filter(col("originContinent") == col("destinationContinent"))  # Filter regional flights
            .groupBy("originContinent", "airlineCode", "airline_name")      # Group by continent and airline
            .count()                                                       # Count regional flights
            .withColumnRenamed("count", "regional_flight_count")           # Rename the count column
            .withColumn(                                                  # Add ranking by regional flight count
                "rank",
                row_number().over(Window.partitionBy("originContinent").orderBy(col("regional_flight_count").desc()))
            )
            .filter(col("rank") == 1)                                     # Filter to get the airline with the most flights
            .select("originContinent", "airlineCode", "airline_name", "regional_flight_count")
        )

        # Step 7: Save the result
        logging.info("\033[1;34mSaving the result to disk...\033[0m")
        save_kpi_data(
            df=result,
            base_path=RESULTS_PATH,
            folder_name="airline_with_most_regional_flights",
            description="Saving Q2: Airline with the most regional flights per continent..."
        )

        logging.info("\033[1;32mQ2: Airline with the most regional flights per continent successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error during processing: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

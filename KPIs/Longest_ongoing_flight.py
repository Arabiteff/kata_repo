import os
import sys
import logging
import coloredlogs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# Import utility functions
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.load_latest_data import load_latest_data
from utils.save_kpi_data import save_kpi_data
from utils.calculate_distance import calculate_distance
from utils.repetitf_joins import join_flights_with_airports

# Configure logging with colored logs
coloredlogs.install(
    level="INFO",
    fmt="%(asctime)s - %(levelname)s - %(message)s",
    level_styles={
        "info": {"color": "green"},
        "critical": {"color": "red", "bold": True},
        "error": {"color": "red"},
        "warning": {"color": "yellow"},
    },
    field_styles={
        "asctime": {"color": "cyan"},
        "levelname": {"bold": True, "color": "blue"},
    }
)

# Initialize Spark session
logging.info("Initializing Spark session...")
spark = SparkSession.builder.appName("Q3_LongestOngoingFlight").getOrCreate()

# Base paths for loading and saving data
base_extracted_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ingested_data"))
results_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/results"))

def main():
    """
    Main function to calculate the longest ongoing flight.
    """
    try:
        # Step 1: Start logging
        logging.info("\033[1;34mStarting Q3: Longest ongoing flight...\033[0m")

        # Step 2: Load flights and airports data
        logging.info("\033[1;32mLoading flights and airports data...\033[0m")
        df_flights = load_latest_data(spark, base_extracted_path, "flights")
        df_airports = load_latest_data(spark, base_extracted_path, "airports")

        # Step 3: Join flights with airport data
        logging.info("\033[1;32mJoining flights with airport coordinates...\033[0m")
        df_flights = join_flights_with_airports(df_flights, df_airports)

        # Step 4: Calculate flight distances
        logging.info("\033[1;34mCalculating flight distances...\033[0m")
        flights_pd = df_flights.toPandas()
        flights_pd["flightDistance"] = flights_pd.apply(calculate_distance, axis=1)
        df_flights = spark.createDataFrame(flights_pd)

        # Step 5: Filter ongoing flights and find the longest flight
        logging.info("\033[1;32mFiltering ongoing flights and finding the longest flight...\033[0m")
        result = (
            df_flights.filter(col("flightStatus") == "0")
            .orderBy(col("flightDistance").desc())
            .limit(1)
        )

        # Step 6: Save the result
        logging.info("\033[1;34mSaving the longest ongoing flight result...\033[0m")
        save_kpi_data(
            df=result,
            base_path=results_path,
            folder_name="longest_ongoing_flight",
            description="Saving Q3: Longest ongoing flight..."
        )

        logging.info("\033[1;32mQ3: Longest ongoing flight successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

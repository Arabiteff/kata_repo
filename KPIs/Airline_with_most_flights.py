import os
import sys
import logging
import coloredlogs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Import utility functions
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.load_latest_data import load_latest_data
from utils.save_kpi_data import save_kpi_data

# Configure logging with coloredlogs
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

# Base paths for loading and saving data
BASE_EXTRACTED_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ingested_data"))
RESULTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/results"))

def main():
    """
    Main function to identify the airline with the most ongoing flights.
    """
    try:
        logging.info("\033[1;34mInitializing Spark session...\033[0m")
        spark = SparkSession.builder.appName("Q1_AirlineWithMostFlights").getOrCreate()

        # Step 1: Load flight data
        logging.info("\033[1;32mLoading flight data...\033[0m")
        df_flights = load_latest_data(spark, BASE_EXTRACTED_PATH, "flights")

        # Step 2: Load airline data
        logging.info("\033[1;32mLoading airline data...\033[0m")
        df_airlines = load_latest_data(spark, BASE_EXTRACTED_PATH, "airlines")

        # Step 3: Identify the airline with the most ongoing flights
        logging.info("\033[1;34mCalculating the airline with the most ongoing flights...\033[0m")
        top_airline = (
            df_flights
            .filter(col("flightStatus") == '0')  # Filter only ongoing flights
            .groupBy("airlineCode")  # Group by airline code
            .agg(count("*").alias("flight_count"))  # Count the flights
            .orderBy(col("flight_count").desc())  # Sort in descending order
            .limit(1)  # Get the top airline
        )

        # Step 4: Merge with airline data to include airline names
        logging.info("\033[1;32mMerging with airline data to include airline names...\033[0m")
        result = (
            top_airline
            .join(
                df_airlines,
                top_airline["airlineCode"] == df_airlines["ICAO"],
                "inner"
            )
            .select(
                top_airline["airlineCode"],
                df_airlines["Name"].alias("airline_name"),
                top_airline["flight_count"]
            )
        )

        # Step 5: Save the result
        logging.info("\033[1;34mSaving the result...\033[0m")
        save_kpi_data(
            df=result,
            base_path=RESULTS_PATH,
            folder_name="airline_with_most_flights",
            description="Saving Q1: Airline with the most ongoing flights..."
        )

        logging.info("\033[1;32mQ1: Airline with the most ongoing flights successfully calculated and saved.\033[0m")

    except Exception as e:
        logging.critical(f"\033[1;31mCritical error during processing: {e}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    main()

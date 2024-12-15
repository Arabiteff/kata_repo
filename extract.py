import os
import logging
import coloredlogs  # Add coloredlogs for colorful logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import airport_schema, flight_schema, airline_schema, zone_schema
from utils.save_kpi_data import save_kpi_data  # Import the save_kpi_data function
from FlightRadar24 import FlightRadar24API

class FlightPipeline:
    def __init__(self):
        """
        Initializes the FlightPipeline class.
        Sets up the Spark session, FlightRadar24 API, and data structures.
        """
        logging.info("Initializing FlightPipeline...")

        # Initialize the FlightRadar24 API
        self.fr_api = FlightRadar24API()

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName("FlightPipeline").getOrCreate()

        # DataFrame placeholders
        self.df_flights = None
        self.df_airports = None
        self.df_airlines = None
        self.df_zones = None

    def extract(self):
        """
        Extracts data from the FlightRadar24 API for flights, airports, airlines, and zones.
        """
        logging.info("\033[1;34mStarting data extraction...\033[0m")  # Blue for main task
        try:
            # Step 1: Ingest Flights
            logging.info("\033[1;32mExtracting flight data...\033[0m")  # Green for subtasks
            flights = self.fr_api.get_flights()
            data_flights = []

            for flight in flights:
                try:
                    data_flights.append({
                        "flightID": flight.id,
                        "aircraft": flight.aircraft_code,
                        "airlineCode": flight.airline_icao,
                        "airportOrigineCode": flight.origin_airport_iata,
                        "airportDestinationCode": flight.destination_airport_iata,
                        "flightStatus": flight.on_ground
                    })
                except Exception as e:
                    logging.error(f"Error processing flight {flight.id}: {e}")

            self.df_flights = self.spark.createDataFrame(data_flights, schema=flight_schema)

            # Clean flights data
            logging.info("Cleaning flight data...")
            self.df_flights = self.df_flights.filter(
                (col("airlineCode") != "N/A") &
                (col("airlineCode") != "") &
                (col("airportOrigineCode") != "") &
                (col("airportDestinationCode") != "")
            )

            logging.info("Flight data extracted and cleaned.")

            # Step 2: Ingest Airports
            logging.info("\033[1;32mExtracting airport data...\033[0m")
            airports = self.fr_api.get_airports()
            data_airports = [
                {
                    "airportID": airport.iata,
                    "airportName": airport.name,
                    "airportLat": float(airport.latitude) if airport.latitude else None,
                    "airportLong": float(airport.longitude) if airport.longitude else None,
                    "airportCountryName": airport.country,
                }
                for airport in airports
            ]
            self.df_airports = self.spark.createDataFrame(data_airports, schema=airport_schema)
            logging.info("Airport data extracted.")

            # Step 3: Ingest Airlines
            logging.info("\033[1;32mExtracting airline data...\033[0m")
            airlines = self.fr_api.get_airlines()
            self.df_airlines = self.spark.createDataFrame(airlines, schema=airline_schema)
            logging.info("Airline data extracted.")

            # Step 4: Ingest Zones
            logging.info("\033[1;32mExtracting zone data...\033[0m")
            zones = self.fr_api.get_zones()
            data_zones = []

            for continent, details in zones.items():
                if "subzones" in details:
                    for subzone, sub_details in details["subzones"].items():
                        data_zones.append({
                            "continent": continent,
                            "subzone": subzone,
                            "tl_y": float(sub_details["tl_y"]),
                            "tl_x": float(sub_details["tl_x"]),
                            "br_y": float(sub_details["br_y"]),
                            "br_x": float(sub_details["br_x"]),
                        })
                else:
                    data_zones.append({
                        "continent": continent,
                        "subzone": None,
                        "tl_y": float(details["tl_y"]),
                        "tl_x": float(details["tl_x"]),
                        "br_y": float(details["br_y"]),
                        "br_x": float(details["br_x"]),
                    })

            self.df_zones = self.spark.createDataFrame(data_zones, schema=zone_schema)
            logging.info("Zone data extracted.")

        except Exception as e:
            logging.critical(f"Critical error during data extraction: {e}")
            raise

    def save(self):
        """
        Saves ingested data to Parquet files in a structured directory.
        """
        logging.info("\033[1;34mStarting data saving process...\033[0m")
        try:
            output_base_path = "./data/ingested_data"

            if self.df_flights is not None:
                save_kpi_data(
                    df=self.df_flights,
                    base_path=output_base_path,
                    folder_name="flights",
                    description="\033[1;36mSaving flights data...\033[0m"
                )

            if self.df_airports is not None:
                save_kpi_data(
                    df=self.df_airports,
                    base_path=output_base_path,
                    folder_name="airports",
                    description="\033[1;36mSaving airports data...\033[0m"
                )

            if self.df_airlines is not None:
                save_kpi_data(
                    df=self.df_airlines,
                    base_path=output_base_path,
                    folder_name="airlines",
                    description="\033[1;36mSaving airlines data...\033[0m"
                )

            if self.df_zones is not None:
                save_kpi_data(
                    df=self.df_zones,
                    base_path=output_base_path,
                    folder_name="zones",
                    description="\033[1;36mSaving zones data...\033[0m"
                )

            logging.info("\033[1;32mAll data successfully saved.\033[0m")

        except Exception as e:
            logging.critical(f"Critical error during data saving: {e}")
            raise

if __name__ == "__main__":
    # Initialize logging with coloredlogs
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

    logging.info("\033[1;34mStarting FlightPipeline...\033[0m")
    pipeline = FlightPipeline()
    pipeline.extract()
    pipeline.save()
    logging.info("\033[1;34mFlightPipeline completed successfully.\033[0m")

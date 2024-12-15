from FlightRadar24 import FlightRadar24API
import pandas as pd
from utils.calculate_distance import calculate_distance
from utils.map_zone import map_zone
import os
from datetime import datetime
import logging
import json

class FlightPipeline:
    def __init__(self):
        self.fr_api = FlightRadar24API()
        self.df_flights = None
        self.df_airports = None
        self.df_airlines = None
        self.df_zones = None

        # Initialize logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("pipeline.log"),
                logging.StreamHandler()
            ]
        )

    def extract(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        ingested_data_path = "./data/ingested_data"
        os.makedirs(ingested_data_path, exist_ok=True)

        try:
            # Step 1: Ingest Flights
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
                    logging.error(f"Error retrieving flight details for flight {flight}: {e}")
            self.df_flights = pd.DataFrame(data_flights)
            self.df_flights = self.df_flights[(self.df_flights != "").all(axis=1)]
            self.df_flights.to_csv(os.path.join(ingested_data_path, f"flights_{timestamp}.csv"), index=False)
            logging.info('Step 1: Ingest Flights Finished')

            # Step 2: Ingest Airports
            airports = self.fr_api.get_airports()
            data_airports = []
            for airport in airports:
                try:
                    data_airports.append({
                        "airportID": airport.iata,
                        "airportName": airport.name,
                        "airportLat": airport.latitude,
                        "airportLong": airport.longitude,
                        "airportCountryName": airport.country,
                    })
                except Exception as e:
                    logging.error(f"Error retrieving airport details for airport {airport}: {e}")
            self.df_airports = pd.DataFrame(data_airports)
            self.df_airports.to_csv(os.path.join(ingested_data_path, f"airports_{timestamp}.csv"), index=False)
            logging.info('Step 2: Ingest Airports Finished')

            # Step 3: Ingest Airlines
            airlines = self.fr_api.get_airlines()
            self.df_airlines = pd.DataFrame(airlines)
            self.df_airlines.to_csv(os.path.join(ingested_data_path, f"airlines_{timestamp}.csv"), index=False)
            logging.info('Step 3: Ingest Airlines Finished')

            # Step 4: Ingest Zones
            zones = self.fr_api.get_zones()
            data_zones = []
            for continent, details in zones.items():
                if "subzones" in details:
                    for subzone, sub_details in details["subzones"].items():
                        data_zones.append({
                            "continent": continent,
                            "subzone": subzone,
                            "tl_y": sub_details["tl_y"],
                            "tl_x": sub_details["tl_x"],
                            "br_y": sub_details["br_y"],
                            "br_x": sub_details["br_x"]
                        })
                else:
                    data_zones.append({
                        "continent": continent,
                        "subzone": None,
                        "tl_y": details["tl_y"],
                        "tl_x": details["tl_x"],
                        "br_y": details["br_y"],
                        "br_x": details["br_x"]
                    })
            self.df_zones = pd.DataFrame(data_zones)
            self.df_zones.to_csv(os.path.join(ingested_data_path, f"zones_{timestamp}.csv"), index=False)
            logging.info('Step 4: Ingest Zones Finished')

        except Exception as e:
            logging.critical(f"Critical error during ingestion: {e}")

    def transform(self):
        try:
            # Step 5: Join Data
            self.df_flights = self.df_flights.merge(self.df_airports, left_on="airportOrigineCode", right_on="airportID", how="left", suffixes=("", "_origin"))
            self.df_flights = self.df_flights.merge(self.df_airports, left_on="airportDestinationCode", right_on="airportID", how="left", suffixes=("", "_destination"))
            print(self.df_flights)
            logging.info('Step 5: Join Data Finished')

            # Step 6: Add Distance Calculation
            self.df_flights["flightDistance"] = self.df_flights.apply(calculate_distance, axis=1)
            logging.info('Step 6: Add Distance Calculation Finished')

            # Step 7: Map Zones to Flights
            self.df_flights["originContinent"] = self.df_flights.apply(
                lambda row: map_zone(row["airportLat"], row["airportLong"], self.df_zones) if pd.notna(row["airportLat"]) else None, axis=1
            )
            self.df_flights["destinationContinent"] = self.df_flights.apply(
                lambda row: map_zone(row["airportLat_destination"], row["airportLong_destination"], self.df_zones) if pd.notna(row["airportLat_destination"]) else None, axis=1
            )
            logging.info('Step 7: Map Zones to Flights Finished')

            # Step 8: Answer Questions
            results = {}

            # Q1: The airline with the most ongoing flights
            airline_with_most_flights = self.df_flights[self.df_flights["flightStatus"] == 0]["airlineCode"].value_counts().idxmax()
            results["Q1"] = {
                "description": "The airline with the most ongoing flights",
                "result": airline_with_most_flights
            }
            logging.info('Q1: The airline with the most ongoing flights finished')

            # Q2: For each continent, the airline with the most regional flights
            regional_flights = self.df_flights[self.df_flights["originContinent"] == self.df_flights["destinationContinent"]]
            regional_airline_counts = regional_flights.groupby(["originContinent", "airlineCode"]).size().reset_index(name="count")
            regional_airline_with_most_flights = regional_airline_counts.loc[regional_airline_counts.groupby("originContinent")["count"].idxmax()]
            results["Q2"] = {
                "description": "For each continent, the airline with the most regional flights",
                "result": regional_airline_with_most_flights.to_dict(orient="records")
            }
            logging.info('Q2: For each continent, the airline with the most regional flights finished')

            # Q3: The longest ongoing flight
            longest_flight = self.df_flights[self.df_flights["flightStatus"] == 0].sort_values(by="flightDistance", ascending=False).iloc[0]
            results["Q3"] = {
                "description": "The longest ongoing flight",
                "result": longest_flight.to_dict()
            }
            logging.info('Q3: The longest ongoing flight finished')

            # Q4: Average flight distance by continent
            avg_distance_by_continent = self.df_flights.groupby("originContinent")["flightDistance"].mean()
            results["Q4"] = {
                "description": "Average flight distance by continent",
                "result": avg_distance_by_continent.to_dict()
            }
            logging.info('Q4: Average flight distance by continent finished')

            return results

        except Exception as e:
            logging.critical(f"Critical error during analysis: {e}")
            return {}

    def loading(self, results):
        try:
            results_path = "./data/results"
            os.makedirs(results_path, exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

            # Save results to JSON
            with open(os.path.join(results_path, f"results_{timestamp}.json"), "w") as json_file:
                json.dump(results, json_file, indent=4)
            logging.info('Results saved successfully')

        except Exception as e:
            logging.critical(f"Critical error during loading: {e}")

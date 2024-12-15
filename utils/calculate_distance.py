import pandas as pd
from math import radians, sin, cos, acos

# def calculate_distance(row):
#     if pd.notna(row["airportLat"]) and pd.notna(row["airportLong"]) and pd.notna(row["airportLat_destination"]) and pd.notna(row["airportLong_destination"]):
#         lat1, lon1 = row["airportLat"], row["airportLong"]
#         lat2, lon2 = row["airportLat_destination"], row["airportLong_destination"]
#         lat1, lon1 = radians(lat1), radians(lon1)
#         lat2, lon2 = radians(lat2), radians(lon2)
#         return acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371
#     return None


def calculate_distance(row):
    if pd.notna(row["airportLat_origin"]) and pd.notna(row["airportLong_origin"]) and pd.notna(row["airportLat_destination"]) and pd.notna(row["airportLong_destination"]):
        lat1, lon1 = row["airportLat_origin"], row["airportLong_origin"]
        lat2, lon2 = row["airportLat_destination"], row["airportLong_destination"]
        lat1, lon1 = radians(lat1), radians(lon1)
        lat2, lon2 = radians(lat2), radians(lon2)
        # Ensure input to acos is within valid range [-1, 1]
        cos_theta = sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)
        cos_theta = max(-1, min(1, cos_theta))  # Clamp value
        return acos(cos_theta) * 6371
    return None

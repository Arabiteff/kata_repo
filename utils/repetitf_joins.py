from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def join_flights_with_airports(df_flights: DataFrame, df_airports: DataFrame) -> DataFrame:
    """
    Joins the flights DataFrame with the airports DataFrame to add origin and destination coordinates.

    Args:
        df_flights (DataFrame): The flights DataFrame.
        df_airports (DataFrame): The airports DataFrame.

    Returns:
        DataFrame: The resulting DataFrame with origin and destination coordinates.
    """
    return (
        df_flights
        .join(
            df_airports.select(
                col("airportID").alias("airportID_origin"),
                col("airportLat").alias("airportLat_origin"),
                col("airportLong").alias("airportLong_origin"),
            ),
            df_flights["airportOrigineCode"] == col("airportID_origin"),
            "left"
        )
        .join(
            df_airports.select(
                col("airportID").alias("airportID_destination"),
                col("airportLat").alias("airportLat_destination"),
                col("airportLong").alias("airportLong_destination"),
            ),
            df_flights["airportDestinationCode"] == col("airportID_destination"),
            "left"
        )
    )

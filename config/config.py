from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Schema for Airports
airport_schema = StructType([
    StructField("airportID", StringType(), True),
    StructField("airportName", StringType(), True),
    StructField("airportLat", DoubleType(), True),
    StructField("airportLong", DoubleType(), True),
    StructField("airportCountryName", StringType(), True),
])

# Schema for Flights
flight_schema = StructType([
    StructField("flightID", StringType(), True),
    StructField("aircraft", StringType(), True),
    StructField("airlineCode", StringType(), True),
    StructField("airportOrigineCode", StringType(), True),
    StructField("airportDestinationCode", StringType(), True),
    StructField("flightStatus", StringType(), True),
])

# Schema for Airlines
airline_schema = StructType([
    StructField("Code", StringType()),
    StructField("ICAO", StringType()),
    StructField("Name", StringType()),
])

# Schema for Zones
zone_schema = StructType([
    StructField("continent", StringType(), True),
    StructField("subzone", StringType(), True),
    StructField("tl_y", DoubleType(), True),
    StructField("tl_x", DoubleType(), True),
    StructField("br_y", DoubleType(), True),
    StructField("br_x", DoubleType(), True)
])
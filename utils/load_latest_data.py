import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession

def load_latest_data(spark, base_path, table):
    """
    Load the latest Parquet data for the specified table based on the current datetime.
    
    Args:
        spark (SparkSession): Active Spark session.
        base_path (str): Base path where the data is stored.
        table (str): Name of the table to load data from.

    Returns:
        pyspark.sql.DataFrame: Spark DataFrame containing the loaded data.
    """
    try:
        # Get the current datetime for partition values
        now = datetime.now()
        tech_year = now.strftime("%Y")
        tech_month = now.strftime("%m")
        tech_day = now.strftime("%d")
        tech_hour = now.strftime("%H")

        # Construct the table path
        table_path = os.path.join(
            base_path, 
            table, 
            f"tech_year={tech_year}", 
            f"tech_month={tech_month}", 
            f"tech_day={tech_day}", 
            f"tech_hour={tech_hour}"
        )
        logging.info(f"Loading data for {table} from {table_path}")
        return spark.read.parquet(table_path)
    except Exception as e:
        logging.error(f"Error loading data for {table} from {table_path}: {e}")
        raise

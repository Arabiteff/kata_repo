import os
import logging
from datetime import datetime

def save_kpi_data(df, base_path, folder_name, description=None):
    """
    Save a DataFrame to a structured folder based on the current datetime and log details.

    Args:
        df (DataFrame): The DataFrame to save.
        base_path (str): The base path where the data should be saved.
        folder_name (str): The folder name under which the data should be saved.
        description (str, optional): Optional description to log before saving.

    Returns:
        str: The full path where the data was saved.
    """
    try:
        if description:
            logging.info(description)
        
        # Get the current datetime for partitioning
        now = datetime.now()
        tech_year = now.strftime("%Y")
        tech_month = now.strftime("%m")
        tech_day = now.strftime("%d")
        tech_hour = now.strftime("%H")
        
        # Construct the output path
        output_path = os.path.join(
            base_path, 
            folder_name,
            f"tech_year={tech_year}",
            f"tech_month={tech_month}",
            f"tech_day={tech_day}",
            f"tech_hour={tech_hour}"
        )
        
        # Ensure the directory exists
        os.makedirs(output_path, exist_ok=True)

        # Save the DataFrame
        logging.info(f"Saving results to {output_path}...")
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        logging.info(f"Data successfully saved to {output_path}.")
        
        return output_path
    except Exception as e:
        logging.error(f"Error saving data to {output_path}: {e}")
        raise

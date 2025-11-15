import os

from dotenv import load_dotenv

from nc_tables_api_wrapper.fetch_table import fetch_table_data
from nc_tables_api_wrapper.upload_table import upload_to_table
from transform.transform_data import create_family_hours_table

# Load environment variables from .env file
load_dotenv()

# Environment variables for table IDs
HOURS_TABLE_ID = os.getenv("HOURS_TABLE_ID")
NAMES_TABLE_ID = os.getenv("NAMES_TABLE_ID")
FAMILY_HOURS_TABLE_ID = os.getenv("FAMILY_HOURS_TABLE_ID")

if __name__ == "__main__":
    print("Fetching source tables...")
    hours_df = fetch_table_data(table_id=HOURS_TABLE_ID, explode=True)
    names_df = fetch_table_data(table_id=NAMES_TABLE_ID)
    print("Transforming data...")
    family_hours_df = create_family_hours_table(hours_df, names_df)
    print("Uploading transformed data...")
    upload_to_table(
        table_id=FAMILY_HOURS_TABLE_ID,
        dataframe=family_hours_df,
        replace=True,
    )
    print("Done.")

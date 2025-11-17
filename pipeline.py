import os
from datetime import datetime

from dotenv import load_dotenv

from nc_tables_api_wrapper.fetch_table import fetch_table_data
from nc_tables_api_wrapper.table_properties import write_table_properties
from nc_tables_api_wrapper.upload_table import upload_to_table
from transform.transform_data import create_family_hours_table

# Load environment variables from .env file
load_dotenv()

# Environment variables for table IDs
HOURS_TABLE_ID = int(os.getenv("HOURS_TABLE_ID"))  # pyright: ignore[reportArgumentType]
NAMES_TABLE_ID = int(os.getenv("NAMES_TABLE_ID"))  # pyright: ignore[reportArgumentType]
FAMILY_HOURS_TABLE_ID = int(os.getenv("FAMILY_HOURS_TABLE_ID"))  # pyright: ignore[reportArgumentType]
KITA_YEAR = int(os.getenv("KITA_YEAR", "2025"))

if __name__ == "__main__":
    print("Fetching source tables...")
    hours_df = fetch_table_data(table_id=HOURS_TABLE_ID, explode=True)
    names_df = fetch_table_data(table_id=NAMES_TABLE_ID)
    print("Transforming data...")
    family_hours_df = create_family_hours_table(
        df_hours=hours_df,
        df_names=names_df,
        kita_year=KITA_YEAR,
    )
    print("Uploading transformed data...")
    upload_to_table(
        table_id=FAMILY_HOURS_TABLE_ID,
        dataframe=family_hours_df,
        replace=True,
    )
    print("Updating table properties...")
    timestamp = datetime.now().strftime("%d.%m.%Y um %H:%M Uhr")
    kita_year_progress_percent = int(
        (datetime.now() - datetime(KITA_YEAR, 9, 1)).days / 365 * 100
    )
    write_table_properties(
        table_id=FAMILY_HOURS_TABLE_ID,
        properties={
            "description": f"Automatisch aktualisiert am {timestamp}.<br>"
            f"Fortschritt des Kita-Jahres {KITA_YEAR}/{KITA_YEAR + 1}: {kita_year_progress_percent}%.",
            "title": f"Stundenliste {KITA_YEAR}/{KITA_YEAR + 1}",
            "emoji": "📊",
        },
    )
    print("Done.")

"""Tool to fetch data from a Nextcloud Tables API endpoint.
OpenAPI definition available at https://raw.githubusercontent.com/nextcloud/tables/main/openapi.json
"""

import os

import dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

dotenv.load_dotenv()

BASE_URL = os.getenv("BASE_URL", "")
NEXTCLOUD_USER = os.getenv("NEXTCLOUD_USER", "")
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "")


def fetch_data(url: str) -> dict:
    """Fetch data from the Nextcloud Tables API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.

    Returns:
        dict: The JSON response from the API.
    """
    response = requests.get(url, auth=HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD))
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()


def fetch_table_data(table_id: int) -> pd.DataFrame:
    """Fetch table data from the Nextcloud Tables API and return as a DataFrame.
    Fetches all rows from the specified table and uses the proper column labels.

    Args:
        table_id (int): The ID of the table to fetch data from.

    Returns:
        pd.DataFrame: The table data as a pandas DataFrame.
    """
    # Get column definitions to understand data structure and column titles
    columns_data = fetch_data(f"{BASE_URL}/index.php/apps/tables/api/1/tables/{table_id}/columns")

    # Create a mapping from column ID to column title
    column_mapping = {}
    for col in columns_data:
        column_mapping[col["id"]] = col["title"]

    # Get all rows from the table
    # Start with initial request to get all data
    all_rows = []
    limit = 100  # Reasonable batch size
    offset = 0

    while True:
        # Fetch rows in batches to handle large tables
        rows_data = fetch_data(
            f"{BASE_URL}/index.php/apps/tables/api/1/tables/{table_id}/rows?limit={limit}&offset={offset}"
        )

        if not rows_data:
            break

        all_rows.extend(rows_data)

        # If we got fewer rows than the limit, we've reached the end
        if len(rows_data) < limit:
            break

        offset += limit

    # Convert rows to DataFrame format
    if not all_rows:
        # Return empty DataFrame with column names if no data
        return pd.DataFrame(columns=list(column_mapping.values()))

    # Process rows into a list of dictionaries with proper column names
    processed_rows = []
    for row in all_rows:
        row_dict = {}
        # Each row has a 'data' field containing a list of column data objects
        if "data" in row and row["data"]:
            if isinstance(row["data"], list):
                # Data is a list of objects with columnId and value
                for data_item in row["data"]:
                    if (
                        isinstance(data_item, dict)
                        and "columnId" in data_item
                        and "value" in data_item
                    ):
                        col_id = data_item["columnId"]
                        value = data_item["value"]
                        if col_id in column_mapping:
                            column_name = column_mapping[col_id]
                            row_dict[column_name] = value
            elif isinstance(row["data"], dict):
                # Data is a dictionary with column_id -> value mappings
                for col_id, value in row["data"].items():
                    col_id_int = int(col_id)
                    if col_id_int in column_mapping:
                        column_name = column_mapping[col_id_int]
                        # Handle different value formats - some columns store complex objects
                        if isinstance(value, dict) and "value" in value:
                            row_dict[column_name] = value["value"]
                        else:
                            row_dict[column_name] = value
        processed_rows.append(row_dict)

    # Create DataFrame
    df = pd.DataFrame(processed_rows)

    # Ensure all columns are present (fill with None for missing columns)
    for column_name in column_mapping.values():
        if column_name not in df.columns:
            df[column_name] = None

    # Reorder columns to match the order from the API
    ordered_columns = [column_mapping[col["id"]] for col in columns_data]
    df = df.reindex(columns=ordered_columns)

    return df


if __name__ == "__main__":
    table_hours = fetch_table_data(table_id=13)
    table_names = fetch_table_data(table_id=8)

    table_hours.to_csv("hours.csv", index=False)
    table_names.to_csv("names.csv", index=False)

    # TODO merge tables and process data

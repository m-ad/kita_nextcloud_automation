"""Tool to fetch data from a Nextcloud Tables API endpoint.
OpenAPI definition available at https://raw.githubusercontent.com/nextcloud/tables/main/openapi.json
"""

import ast
import os

import dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

dotenv.load_dotenv()

BASE_URL = os.getenv("BASE_URL", "")
NEXTCLOUD_USER = os.getenv("NEXTCLOUD_USER", "")
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "")


def _parse_complex_value(value):
    """Parse a value that might be a string representation of a list or already a list."""
    if isinstance(value, str):
        # Try to parse string representation of a list/dict
        try:
            # Replace single quotes with double quotes for JSON parsing
            if value.strip().startswith("[") or value.strip().startswith("{"):
                parsed = ast.literal_eval(value)
                return parsed
        except (ValueError, SyntaxError):
            # If parsing fails, return as-is
            pass
    return value


def fetch_data(endpoint: str) -> dict:
    """Fetch data from the Nextcloud Tables API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.

    Returns:
        dict: The JSON response from the API.
    """
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, auth=HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD))
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()


def _build_column_mappings(columns_data):
    """Build column mappings from API response.

    Returns:
        tuple: (column_mapping, select_mappings)
    """
    column_mapping = {}
    select_mappings = {}

    for col in columns_data:
        column_mapping[col["id"]] = col["title"]

        # Handle select/selection type columns
        if col.get("type") in ["select", "selection"] and "selectionOptions" in col:
            select_options = {}
            for option in col["selectionOptions"]:
                # Map option ID to its label/text
                option_id = option.get("id")
                option_label = (
                    option.get("label") or option.get("text") or option.get("title")
                )
                if option_id is not None and option_label:
                    select_options[option_id] = option_label
            if select_options:
                select_mappings[col["id"]] = select_options

    return column_mapping, select_mappings


def _fetch_all_rows(table_id, limit=100):
    """Fetch all rows from a table using pagination."""
    all_rows = []
    offset = 0

    while True:
        # Fetch rows in batches to handle large tables
        rows_data = fetch_data(
            f"index.php/apps/tables/api/1/tables/{table_id}/rows?limit={limit}&offset={offset}"
        )

        if not rows_data:
            break

        all_rows.extend(rows_data)

        # If we got fewer rows than the limit, we've reached the end
        if len(rows_data) < limit:
            break

        offset += limit

    return all_rows


def _process_value(value, col_id, select_mappings):
    """Process a single value, handling complex parsing and select mappings."""
    # Parse complex values (string representations of lists/dicts)
    parsed_value = _parse_complex_value(value)

    # Handle select type columns - convert ID to label
    if col_id in select_mappings and isinstance(parsed_value, (int, str)):
        try:
            option_id = int(parsed_value)
            if option_id in select_mappings[col_id]:
                parsed_value = select_mappings[col_id][option_id]
        except (ValueError, TypeError):
            # If conversion fails, keep original value
            pass

    return parsed_value


def _process_row_data(row, column_mapping, select_mappings, explode):
    """Process a single row of data into base and explodable components."""
    base_row_dict = {}
    explodable_columns = {}

    if "data" not in row or not row["data"]:
        return base_row_dict, explodable_columns

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
                    parsed_value = _process_value(value, col_id, select_mappings)

                    # Check if value is a list of dicts (complex data)
                    if (
                        explode
                        and isinstance(parsed_value, list)
                        and parsed_value
                        and isinstance(parsed_value[0], dict)
                    ):
                        explodable_columns[column_name] = parsed_value
                    else:
                        base_row_dict[column_name] = parsed_value

    elif isinstance(row["data"], dict):
        # Data is a dictionary with column_id -> value mappings
        for col_id, value in row["data"].items():
            col_id_int = int(col_id)
            if col_id_int in column_mapping:
                column_name = column_mapping[col_id_int]
                # Handle different value formats - some columns store complex objects
                if isinstance(value, dict) and "value" in value:
                    actual_value = value["value"]
                else:
                    actual_value = value

                parsed_value = _process_value(actual_value, col_id_int, select_mappings)

                # Check if value is a list of dicts (complex data)
                if (
                    explode
                    and isinstance(parsed_value, list)
                    and parsed_value
                    and isinstance(parsed_value[0], dict)
                ):
                    explodable_columns[column_name] = parsed_value
                else:
                    base_row_dict[column_name] = parsed_value

    return base_row_dict, explodable_columns


def _explode_row(base_row_dict, explodable_columns):
    """Explode a row with complex data into multiple rows."""
    processed_rows = []

    # Find the maximum number of items in any explodable column
    max_items = max(len(items) for items in explodable_columns.values())

    # Create a row for each item in the exploded data
    for i in range(max_items):
        row_dict = base_row_dict.copy()

        for col_name, items in explodable_columns.items():
            if i < len(items):
                item = items[i]
                if isinstance(item, dict):
                    # Flatten the dictionary keys as columns
                    for key, val in item.items():
                        flattened_col_name = f"{col_name}_{key}"
                        row_dict[flattened_col_name] = val
                else:
                    row_dict[col_name] = item
            # If this column has fewer items, leave as None

        processed_rows.append(row_dict)

    return processed_rows


def _finalize_dataframe(df, columns_data, column_mapping, explode):
    """Finalize the DataFrame with proper column ordering and missing columns."""
    # Ensure all original columns are present (fill with None for missing columns)
    for column_name in column_mapping.values():
        if column_name not in df.columns:
            df[column_name] = None

    # For non-exploded data, reorder columns to match the order from the API
    if not explode:
        ordered_columns = [column_mapping[col["id"]] for col in columns_data]
        df = df.reindex(columns=ordered_columns)
    else:
        # For exploded data, put original columns first, then exploded columns
        original_columns = [column_mapping[col["id"]] for col in columns_data]
        exploded_columns = [col for col in df.columns if col not in original_columns]
        new_order = original_columns + exploded_columns
        df = df.reindex(columns=new_order)

    return df


def fetch_table_data(table_id: int, explode: bool = False) -> pd.DataFrame:
    """Fetch table data from the Nextcloud Tables API and return as a DataFrame.
    Fetches all rows from the specified table and uses the proper column labels.

    Args:
        table_id (int): The ID of the table to fetch data from.
        explode (bool): If True, explode columns containing lists of dictionaries
                       into separate rows with flattened dictionary keys as columns.

    Returns:
        pd.DataFrame: The table data as a pandas DataFrame.
    """
    # Get column definitions and build mappings
    columns_data = fetch_data(f"index.php/apps/tables/api/1/tables/{table_id}/columns")
    column_mapping, select_mappings = _build_column_mappings(columns_data)

    # Fetch all rows from the table
    all_rows = _fetch_all_rows(table_id)

    # Handle empty table case
    if not all_rows:
        return pd.DataFrame(columns=list(column_mapping.values()))

    # Process each row
    processed_rows = []
    for row in all_rows:
        base_row_dict, explodable_columns = _process_row_data(
            row, column_mapping, select_mappings, explode
        )

        # Handle exploding if there are explodable columns
        if explode and explodable_columns:
            processed_rows.extend(_explode_row(base_row_dict, explodable_columns))
        else:
            processed_rows.append(base_row_dict)

    # Create and finalize DataFrame
    df = pd.DataFrame(processed_rows)
    return _finalize_dataframe(df, columns_data, column_mapping, explode)


if __name__ == "__main__":
    # Fetch data without exploding
    table_hours = fetch_table_data(table_id=13, explode=False)
    table_names = fetch_table_data(table_id=8, explode=False)

    table_hours.to_csv("hours.csv", index=False)
    table_names.to_csv("names.csv", index=False)

    # Fetch data with exploding for complex columns
    table_hours_exploded = fetch_table_data(table_id=13, explode=True)
    table_hours_exploded.to_csv("hours_exploded.csv", index=False)

    # TODO merge tables and process data

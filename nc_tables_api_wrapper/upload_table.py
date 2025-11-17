"""Simple helper for uploading pandas DataFrames into Nextcloud Tables.
OpenAPI definition available at https://raw.githubusercontent.com/nextcloud/tables/main/openapi.json
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List

import dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

dotenv.load_dotenv()

BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
NEXTCLOUD_USER = os.getenv("NEXTCLOUD_USER", "")
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "")


def _require_credentials() -> HTTPBasicAuth:
    """Return the HTTP auth object, ensuring configuration exists."""
    if not BASE_URL:
        raise RuntimeError("BASE_URL is not configured")
    if not NEXTCLOUD_USER or not NEXTCLOUD_PASSWORD:
        raise RuntimeError("NEXTCLOUD_USER or NEXTCLOUD_PASSWORD is not configured")
    return HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD)


def _request(method: str, endpoint: str, **kwargs) -> requests.Response:
    """Execute an authenticated HTTP request against the Tables API."""
    auth = _require_credentials()
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    response = requests.request(method, url, auth=auth, **kwargs)
    response.raise_for_status()
    return response


def _get_columns(table_id: int) -> List[Dict[str, Any]]:
    """Return the column definitions for a table."""
    response = _request("GET", f"index.php/apps/tables/api/1/tables/{table_id}/columns")
    return response.json()


def _build_column_map(columns: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """Map column titles to their identifiers."""
    return {column["title"]: column["id"] for column in columns}


def _normalize_value(value: Any) -> Any:
    """Convert pandas/numpy scalars and NaNs into JSON-friendly values."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value
    return value


def _iter_row_payloads(
    dataframe: pd.DataFrame, column_map: Dict[str, int]
) -> Iterable[Dict[str, Any]]:
    """Yield Tables API payload dictionaries for each DataFrame row."""
    for _, row in dataframe.iterrows():
        payload: Dict[str, Any] = {}
        for column_name, raw_value in row.items():
            if column_name not in column_map:
                continue
            normalized = _normalize_value(raw_value)
            if normalized is None:
                continue
            payload[str(column_map[column_name])] = normalized
        if payload:
            yield {"data": payload}


def clear_table(table_id: int, batch_size: int = 100) -> int:
    """Delete all rows in a table.

    Parameters
    ----------
    table_id:
        Target table identifier.
    batch_size:
        Number of rows to fetch per round-trip while clearing.

    Returns
    -------
    int
        Number of rows that were deleted.
    """

    deleted = 0
    while True:
        response = _request(
            "GET",
            f"index.php/apps/tables/api/1/tables/{table_id}/rows?limit={batch_size}&offset=0",
        )
        rows = response.json()
        if not rows:
            break

        for row in tqdm(rows, desc="Deleting rows"):
            row_id = row.get("id")
            if row_id is None:
                continue
            _request("DELETE", f"index.php/apps/tables/api/1/rows/{row_id}")
            deleted += 1

    return deleted


def upload_to_table(
    table_id: int, dataframe: pd.DataFrame, *, replace: bool = False
) -> List[int]:
    """Upload a DataFrame into a Nextcloud table.

    Parameters
    ----------
    table_id:
        Target table identifier.
    dataframe:
        Data to upload. Column names must match the column titles configured
        in Nextcloud Tables. Values for selection columns must already contain
        the expected option identifiers.
    replace:
        When ``True``, the table is cleared before inserting new rows.

    Returns
    -------
    list[int]
        Row identifiers created by the API.

    Example
    -------
    >>> df = pd.DataFrame(
    ...     [
    ...         {"Child": "Alice", "Hours": 3.5, "Status": 12},
    ...         {"Child": "Bob", "Hours": 2.0, "Status": 15},
    ...     ]
    ... )
    >>> upload_to_table(table_id=13, dataframe=df, replace=True)
    [101, 102]

    The table must already define the columns "Child", "Hours", and "Status".
    For selection columns ("Status" above), look up the numeric option IDs
    via ``fetch_table_data`` from ``fetch_data.py`` and map your human-readable
    labels to those IDs before uploading.
    """

    if dataframe.empty:
        return []

    print("Get columns...")
    columns = _get_columns(table_id)
    column_map = _build_column_map(columns)

    unknown_columns = set(dataframe.columns) - set(column_map)
    if unknown_columns:
        raise ValueError(
            "DataFrame contains columns that do not exist in the target table: "
            + ", ".join(sorted(unknown_columns))
        )

    if replace:
        print("Clearing table...")
        clear_table(table_id)

    created_row_ids: List[int] = []
    for payload in tqdm(
        _iter_row_payloads(dataframe, column_map),
        desc="Uploading rows",
        total=len(dataframe),
    ):
        response = _request(
            "POST", f"index.php/apps/tables/api/1/tables/{table_id}/rows", json=payload
        )
        row = response.json()
        row_id = row.get("id")
        if row_id is not None:
            created_row_ids.append(row_id)

    return created_row_ids


if __name__ == "__main__":
    # Example usage: read a CSV and push its contents to a table.
    from pathlib import Path

    csvfile = Path(__file__).parent.parent / "family_hours_report.csv"
    if os.path.exists(csvfile):
        hours_df = pd.read_csv(csvfile)
        upload_to_table(table_id=72, dataframe=hours_df, replace=True)

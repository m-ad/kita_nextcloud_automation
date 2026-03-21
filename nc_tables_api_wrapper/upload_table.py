"""Simple helper for uploading pandas DataFrames into Nextcloud Tables.
OpenAPI definition available at https://raw.githubusercontent.com/nextcloud/tables/main/openapi.json
"""

from __future__ import annotations

import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, cast

import pandas as pd
from tqdm import tqdm

from ._client import NEXTCLOUD_USER
from ._client import request as _request


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
            column_key = cast(str, column_name)
            if column_key not in column_map:
                continue
            normalized = _normalize_value(raw_value)
            if normalized is None:
                continue
            payload[str(column_map[column_key])] = normalized
        if payload:
            yield {"data": payload}


# ---------------------------------------------------------------------------
# Clearing rows
# ---------------------------------------------------------------------------


def clear_table(table_id: int, batch_size: int = 100, max_workers: int = 5) -> int:
    """Delete all rows in a table using parallel requests.

    Parameters
    ----------
    table_id:
        Target table identifier.
    batch_size:
        Number of rows to fetch per round-trip while collecting IDs.
    max_workers:
        Maximum number of concurrent DELETE requests.

    Returns
    -------
    int
        Number of rows that were deleted.
    """

    # Phase 1: collect all row IDs
    all_row_ids: List[int] = []
    offset = 0
    while True:
        response = _request(
            "GET",
            f"index.php/apps/tables/api/1/tables/{table_id}/rows?limit={batch_size}&offset={offset}",
        )
        rows = response.json()
        if not rows:
            break
        all_row_ids.extend(row["id"] for row in rows if "id" in row)
        if len(rows) < batch_size:
            break
        offset += batch_size

    if not all_row_ids:
        return 0

    # Phase 2: delete in parallel
    def _delete_row(row_id: int) -> int:
        _request("DELETE", f"index.php/apps/tables/api/1/rows/{row_id}")
        return row_id

    deleted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_delete_row, rid) for rid in all_row_ids]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Deleting rows"
        ):
            future.result()  # raises on failure
            deleted += 1

    return deleted


# ---------------------------------------------------------------------------
# CSV import via WebDAV + import endpoint
# ---------------------------------------------------------------------------

_TEMP_REMOTE_PATH = "/kita_automation_temp_import.csv"


def _webdav_endpoint(remote_path: str) -> str:
    """Return the WebDAV endpoint for *remote_path* (relative to user root)."""
    return f"remote.php/dav/files/{NEXTCLOUD_USER}{remote_path}"


def import_to_table(
    table_id: int,
    dataframe: pd.DataFrame,
    *,
    remote_path: str = _TEMP_REMOTE_PATH,
) -> Dict[str, Any]:
    """Upload a DataFrame as CSV via WebDAV and import it in one API call.

    Parameters
    ----------
    table_id:
        Target table identifier.
    dataframe:
        Data to upload.  Column names must match the target table titles.
    remote_path:
        Nextcloud-internal path (relative to user files root) for the
        temporary CSV.  Cleaned up automatically after import.

    Returns
    -------
    dict
        The ``ImportState`` object returned by the API.
    """

    csv_bytes = dataframe.to_csv(index=False).encode("utf-8")
    webdav_ep = _webdav_endpoint(remote_path)

    # Upload CSV to Nextcloud Files via WebDAV.
    # Clear session cookies first — cookies from prior Tables API calls
    # cause Nextcloud to authenticate via session instead of Basic Auth,
    # which fails on the WebDAV endpoint.
    from ._client import _get_session

    _get_session().cookies.clear()

    _request("PUT", webdav_ep, data=csv_bytes, headers={"Content-Type": "text/csv"})

    try:
        # Trigger server-side import
        response = _request(
            "POST",
            f"index.php/apps/tables/api/1/import/table/{table_id}",
            json={"path": remote_path, "createMissingColumns": False},
        )
        result: Dict[str, Any] = response.json()

        errors = result.get("errors_count", 0) or 0
        if errors:
            warnings.warn(
                f"Import completed with {errors} error(s): {result}",
                stacklevel=2,
            )

        return result
    finally:
        # Always clean up the temporary file
        try:
            _request("DELETE", webdav_ep)
        except Exception:
            warnings.warn(
                f"Failed to clean up temporary import file at {remote_path}",
                stacklevel=2,
            )


# ---------------------------------------------------------------------------
# Row-by-row upload (fallback)
# ---------------------------------------------------------------------------


def _upload_rows_sequentially(
    table_id: int, dataframe: pd.DataFrame, column_map: Dict[str, int]
) -> List[int]:
    """Insert rows one by one — used as fallback when CSV import fails."""
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def upload_to_table(
    table_id: int,
    dataframe: pd.DataFrame,
    *,
    replace: bool = False,
    use_import: bool = True,
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
    use_import:
        When ``True`` (default), use the CSV import endpoint instead of
        inserting rows one by one.  Falls back to row-by-row on failure.

    Returns
    -------
    list[int]
        Row identifiers created by the API (empty list when using import).

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

    # --- Fast path: CSV import ---
    if use_import:
        try:
            print("Importing via CSV...")
            result = import_to_table(table_id, dataframe)
            inserted = result.get("inserted_rows_count", 0) or 0
            print(f"Import complete: {inserted} row(s) inserted.")
            return []
        except Exception as exc:
            warnings.warn(
                f"CSV import failed ({exc}), falling back to row-by-row upload.",
                stacklevel=2,
            )

    # --- Slow path: row-by-row ---
    return _upload_rows_sequentially(table_id, dataframe, column_map)


if __name__ == "__main__":
    # Example usage: read a CSV and push its contents to a table.
    from pathlib import Path

    csvfile = Path(__file__).parent.parent / "family_hours_report.csv"
    if os.path.exists(csvfile):
        hours_df = pd.read_csv(csvfile)
        upload_to_table(table_id=72, dataframe=hours_df, replace=True)

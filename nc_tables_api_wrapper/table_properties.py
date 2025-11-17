"""Helpers for inspecting and editing Nextcloud Tables metadata.
OpenAPI definition available at https://raw.githubusercontent.com/nextcloud/tables/main/openapi.json
"""

from __future__ import annotations

from typing import Any, Dict, List

from ._client import request as _request


def list_tables() -> List[Dict[str, Any]]:
    """Retrieve all tables accessible to the authenticated user.

    Returns
    -------
    list of dict
        Table objects as provided by the Nextcloud Tables API.

    Raises
    ------
    RuntimeError
        If authentication credentials are missing.
    requests.HTTPError
        If the API responds with an error status.
    """
    response = _request("GET", "index.php/apps/tables/api/1/tables")
    return response.json()


def read_table_properties(table_id: int) -> Dict[str, Any]:
    """Fetch metadata for a single table.

    Parameters
    ----------
    table_id:
        Identifier of the table to fetch.

    Returns
    -------
    dict
        Table object as described in the OpenAPI specification.

    Raises
    ------
    RuntimeError
        If authentication credentials are missing.
    requests.HTTPError
        If the API responds with an error status.
    """
    response = _request("GET", f"index.php/apps/tables/api/1/tables/{table_id}")
    return response.json()


def write_table_properties(table_id: int, properties: Dict[str, Any]) -> Dict[str, Any]:
    """Update selected table fields and return the updated table.

    Parameters
    ----------
    table_id:
        Identifier of the table to update.
    properties:
        Key-value pairs accepted by ``PUT /ocs/v2.php/apps/tables/api/2/tables/{id}``.

    Returns
    -------
    dict
        The updated table representation returned by the API.

    Raises
    ------
    RuntimeError
        If authentication credentials are missing.
    requests.HTTPError
        If the API responds with an error status.
    """
    response = _request(
        "PUT",
        f"ocs/v2.php/apps/tables/api/2/tables/{table_id}",
        json=properties,
        ocs=True,
    )
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Nextcloud returned a non-JSON response for the table update: "
            f"{response.text[:200]}"
        ) from exc

    return body.get("ocs", {}).get("data", body)


if __name__ == "__main__":
    from datetime import datetime
    from pprint import pprint

    print("Available tables:")
    for table in list_tables():
        print(f"- {table.get('id')}: {table.get('title')}")

    print("\nProperties of table ID 72:")
    properties = read_table_properties(72)
    pprint(properties)

    print("\nUpdating table description...")

    updated_properties = write_table_properties(
        72,
        {
            "description": f"Updated {datetime.now().isoformat()}",
        },
    )
    pprint(updated_properties)

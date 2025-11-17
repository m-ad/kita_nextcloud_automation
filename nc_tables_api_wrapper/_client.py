"""Shared client utilities for interacting with the Nextcloud Tables API."""

from __future__ import annotations

import os
from typing import Dict, Optional

import dotenv
import requests
from requests.auth import HTTPBasicAuth

dotenv.load_dotenv()

BASE_URL_RAW = os.getenv("BASE_URL", "") or ""
BASE_URL = BASE_URL_RAW.rstrip("/")
NEXTCLOUD_USER = os.getenv("NEXTCLOUD_USER", "") or ""
NEXTCLOUD_PASSWORD = os.getenv("NEXTCLOUD_PASSWORD", "") or ""
API_TIMEOUT = float(os.getenv("NEXTCLOUD_TIMEOUT", "30"))


def require_credentials() -> HTTPBasicAuth:
    """Return HTTP basic authentication configured via environment variables.

    Returns
    -------
    requests.auth.HTTPBasicAuth
        Authenticator configured with the user credentials.

    Raises
    ------
    RuntimeError
        If the base URL or credentials are missing.
    """

    if not BASE_URL and not BASE_URL_RAW:
        raise RuntimeError("BASE_URL is not configured")
    if not NEXTCLOUD_USER or not NEXTCLOUD_PASSWORD:
        raise RuntimeError("NEXTCLOUD_USER or NEXTCLOUD_PASSWORD is not configured")
    return HTTPBasicAuth(NEXTCLOUD_USER, NEXTCLOUD_PASSWORD)


def build_url(endpoint: str, *, strip_base: bool = True) -> str:
    """Construct a fully-qualified API URL for the provided endpoint.

    Parameters
    ----------
    endpoint:
        Relative API path (with or without leading slash).
    strip_base:
        When ``True`` (default) the environment base URL is right-stripped once,
        mirroring the original behavior in ``upload_table`` and ``table_properties``.

    Returns
    -------
    str
        Absolute URL pointing at the requested API resource.
    """

    base_url = BASE_URL if strip_base else BASE_URL_RAW
    trimmed_endpoint = endpoint.lstrip("/")
    if base_url:
        return f"{base_url}/{trimmed_endpoint}"
    return f"/{trimmed_endpoint}"


def request(
    method: str,
    endpoint: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    ocs: bool = False,
    strip_base: bool = True,
    **kwargs,
) -> requests.Response:
    """Perform an authenticated HTTP request against the Tables API.

    Parameters
    ----------
    method:
        HTTP verb such as ``"GET"`` or ``"POST"``.
    endpoint:
        Relative API path.
    headers:
        Optional HTTP headers added to the outgoing request.
    ocs:
        When ``True`` automatically adds ``OCS-APIRequest`` and ``Accept`` headers.
    strip_base:
        Toggle whether to use the right-stripped base URL.
    **kwargs:
        Forwarded to :func:`requests.request`.

    Returns
    -------
    requests.Response
        Response with ``raise_for_status`` already invoked to surface failures early.
    """

    auth = require_credentials()
    url = build_url(endpoint, strip_base=strip_base)
    merged_headers = dict(headers or {})
    if ocs:
        merged_headers.setdefault("OCS-APIRequest", "true")
        merged_headers.setdefault("Accept", "application/json")

    timeout = kwargs.pop("timeout", API_TIMEOUT)
    response = requests.request(
        method,
        url,
        auth=auth,
        headers=merged_headers,
        timeout=timeout,
        **kwargs,
    )
    response.raise_for_status()
    return response

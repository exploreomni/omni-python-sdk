"""Hand-written conveniences on top of the generated client.

This module is NOT generated. scripts/generate.sh preserves it across
regenerations; everything else in this package is overwritten.
"""

import base64
import io
import json
import os
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from .client import AuthenticatedClient

__all__ = ["client_from_env", "run_query_blocking", "wait_query_blocking"]


def client_from_env(env_file: str = ".env", **kwargs: Any) -> AuthenticatedClient:
    """Build an AuthenticatedClient from OMNI_API_KEY / OMNI_BASE_URL.

    Values are read from the environment, falling back to `env_file` if
    present. OMNI_BASE_URL should be the bare instance origin, e.g.
    https://myorg.omniapp.co — generated endpoint paths already include
    /api/... prefixes. Extra kwargs are passed to AuthenticatedClient.
    """
    load_dotenv(dotenv_path=env_file)
    api_key = os.getenv("OMNI_API_KEY")
    base_url = os.getenv("OMNI_BASE_URL")
    if not api_key or not base_url:
        raise ValueError("OMNI_API_KEY and OMNI_BASE_URL must be set (in the environment or in the env file)")
    return AuthenticatedClient(base_url=_trim_base_url(base_url), token=api_key, **kwargs)


def _trim_base_url(base_url: str) -> str:
    """Strip trailing slashes and /api[/v1|/unstable] suffixes so paths don't double up."""
    base_url = base_url.rstrip("/")
    for suffix in ("/api/v1", "/api/unstable", "/api"):
        if base_url.endswith(suffix):
            base_url = base_url[: -len(suffix)]
    return base_url


def run_query_blocking(
    client: AuthenticatedClient, body: dict[str, Any], user_id: str | None = None
) -> tuple[pa.Table, list[dict[str, Any]]]:
    """Run a query and block until it completes.

    POSTs /api/v1/query/run and polls /api/v1/query/wait until the job
    finishes, then decodes the NDJSON + base64 Arrow IPC payload.

    Returns (pyarrow.Table, field metadata list).
    """
    http = client.get_httpx_client()
    params = {"userId": user_id} if user_id else None
    response = http.post("/api/v1/query/run", json=body, params=params)
    response.raise_for_status()

    lines = _ndjson(response.text)
    footer = lines[-1]
    done = footer["timed_out"] == "false"
    while not done:
        lines, done = wait_query_blocking(client, footer["remaining_job_ids"])
        footer = lines[-1]

    data_payload = next((line for line in lines if "result" in line), None)
    if data_payload is None:
        raise ValueError("No result found in the query response.")

    raw_arrow_data = base64.b64decode(data_payload["result"])
    with ipc.open_stream(io.BytesIO(raw_arrow_data)) as reader:
        table = reader.read_all()
    return table, data_payload["summary"]["fields"]


def wait_query_blocking(
    client: AuthenticatedClient, remaining_job_ids: list[str]
) -> tuple[list[dict[str, Any]], bool]:
    """Wait on running query jobs via /api/v1/query/wait.

    Returns (parsed NDJSON lines, done flag).
    """
    http = client.get_httpx_client()
    response = http.get("/api/v1/query/wait", params={"job_ids": json.dumps(remaining_job_ids)})
    response.raise_for_status()
    lines = _ndjson(response.text)
    done = lines[-1]["timed_out"] == "false"
    return lines, done


def _ndjson(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]

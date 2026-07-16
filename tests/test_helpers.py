"""Tests for the hand-written helper layer (query -> pyarrow decoding)."""

import base64
import io
import json

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from omni_python_sdk import AuthenticatedClient
from omni_python_sdk.helpers import client_from_env, run_query_blocking, wait_query_blocking


def _arrow_payload(table: pa.Table) -> str:
    buf = io.BytesIO()
    with ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(buf.getvalue()).decode()


TABLE = pa.table({"city": ["Boston", "Denver"], "count": [3, 5]})
FIELDS = [{"fieldName": "city"}, {"fieldName": "count"}]


def _ndjson_response(lines: list[dict]) -> httpx.Response:
    return httpx.Response(200, text="\n".join(json.dumps(line) for line in lines))


def _client_with_transport(handler) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url="https://example.omniapp.co",
        token="test-token",
        httpx_args={"transport": httpx.MockTransport(handler)},
    )


def test_run_query_blocking_decodes_arrow():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query/run"
        assert request.headers["authorization"] == "Bearer test-token"
        return _ndjson_response(
            [
                {"result": _arrow_payload(TABLE), "summary": {"fields": FIELDS}},
                {"timed_out": "false"},
            ]
        )

    table, fields = run_query_blocking(_client_with_transport(handler), {"query": {}})
    assert table.equals(TABLE)
    assert fields == FIELDS


def test_run_query_blocking_polls_wait_until_done():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/api/v1/query/run":
            return _ndjson_response([{"timed_out": "true", "remaining_job_ids": ["job-1"]}])
        assert request.url.path == "/api/v1/query/wait"
        assert json.loads(request.url.params["job_ids"]) == ["job-1"]
        if len(calls) < 3:
            return _ndjson_response([{"timed_out": "true", "remaining_job_ids": ["job-1"]}])
        return _ndjson_response(
            [
                {"result": _arrow_payload(TABLE), "summary": {"fields": FIELDS}},
                {"timed_out": "false"},
            ]
        )

    table, fields = run_query_blocking(_client_with_transport(handler), {"query": {}})
    assert calls == ["/api/v1/query/run", "/api/v1/query/wait", "/api/v1/query/wait"]
    assert table.equals(TABLE)


def test_run_query_blocking_failed_job_raises_with_message():
    def handler(request: httpx.Request) -> httpx.Response:
        return _ndjson_response(
            [
                {"jobs_submitted": {}},
                {
                    "job_id": "job-1",
                    "status": "FAILED",
                    "error_type": "PLAN",
                    "error_message": 'No such view "order_items"',
                },
                {"timed_out": "false", "remaining_job_ids": []},
            ]
        )

    with pytest.raises(ValueError, match=r'Query failed \(PLAN\): No such view "order_items"'):
        run_query_blocking(_client_with_transport(handler), {"query": {}})


def test_run_query_blocking_no_result_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return _ndjson_response([{"timed_out": "false"}])

    with pytest.raises(ValueError, match="No result"):
        run_query_blocking(_client_with_transport(handler), {"query": {}})


def test_run_query_blocking_http_error_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": "forbidden"})

    with pytest.raises(httpx.HTTPStatusError):
        run_query_blocking(_client_with_transport(handler), {"query": {}})


def test_wait_query_blocking_done_flag():
    def handler(request: httpx.Request) -> httpx.Response:
        return _ndjson_response([{"timed_out": "true", "remaining_job_ids": ["job-1"]}])

    lines, done = wait_query_blocking(_client_with_transport(handler), ["job-1"])
    assert done is False
    assert lines[-1]["remaining_job_ids"] == ["job-1"]


def test_client_from_env(tmp_path, monkeypatch):
    monkeypatch.delenv("OMNI_API_KEY", raising=False)
    monkeypatch.delenv("OMNI_BASE_URL", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("OMNI_API_KEY=abc123\nOMNI_BASE_URL=https://example.omniapp.co/api/v1\n")

    client = client_from_env(env_file=str(env_file))
    # /api/v1 suffix is trimmed; token becomes a bearer header
    assert str(client.get_httpx_client().base_url) == "https://example.omniapp.co"
    assert client.get_httpx_client().headers["authorization"] == "Bearer abc123"


def test_client_from_env_missing_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("OMNI_API_KEY", raising=False)
    monkeypatch.delenv("OMNI_BASE_URL", raising=False)
    with pytest.raises(ValueError, match="OMNI_API_KEY"):
        client_from_env(env_file=str(tmp_path / "missing.env"))

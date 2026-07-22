"""Smoke tests for the generated client package."""

import importlib
import pkgutil

import pytest

from omni_python_sdk import AuthenticatedClient, Client
import omni_python_sdk.api as api_pkg


def test_construct_clients():
    client = Client(base_url="https://example.omniapp.co")
    authed = AuthenticatedClient(base_url="https://example.omniapp.co", token="test-token")
    assert client.get_httpx_client().base_url == "https://example.omniapp.co"
    assert authed.get_httpx_client().headers["authorization"] == "Bearer test-token"


@pytest.mark.parametrize(
    "module",
    [
        "omni_python_sdk.api.query.query_run",
        "omni_python_sdk.api.query.query_wait",
        "omni_python_sdk.api.scim.scim_users_list",
        "omni_python_sdk.api.scim.scim_groups_list",
        "omni_python_sdk.api.documents.documents_create",
        "omni_python_sdk.api.whoami.whoami",
    ],
)
def test_endpoint_modules_importable(module):
    mod = importlib.import_module(module)
    assert hasattr(mod, "sync_detailed")
    assert hasattr(mod, "asyncio_detailed")


def test_all_endpoint_modules_import():
    """Every generated endpoint module must import cleanly."""
    count = 0
    for tag in pkgutil.iter_modules(api_pkg.__path__):
        tag_pkg = importlib.import_module(f"omni_python_sdk.api.{tag.name}")
        for op in pkgutil.iter_modules(tag_pkg.__path__):
            importlib.import_module(f"omni_python_sdk.api.{tag.name}.{op.name}")
            count += 1
    assert count >= 190, f"expected ~195 endpoint modules, found {count}"

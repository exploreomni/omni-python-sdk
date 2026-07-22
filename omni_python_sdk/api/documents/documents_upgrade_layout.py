from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_upgrade_layout_body import DocumentsUpgradeLayoutBody
from ...models.documents_upgrade_layout_response import DocumentsUpgradeLayoutResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsUpgradeLayoutBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/documents/{identifier}/upgrade".format(
            identifier=quote(str(identifier), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsUpgradeLayoutResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsUpgradeLayoutResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsUpgradeLayoutResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsUpgradeLayoutBody | Unset = UNSET,
) -> Response[Any | DocumentsUpgradeLayoutResponse]:
    r"""Upgrade dashboard layout

     Upgrades a document to the advanced dashboard layout (the \"File > Upgrade layout\" UI action). No-
    ops when the document already has advanced layout. For published documents the upgrade goes through
    a draft/publish workflow automatically; if a draft already exists, the request returns 409 unless
    `clearExistingDraft` is set to `true`.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpgradeLayoutBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsUpgradeLayoutResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsUpgradeLayoutBody | Unset = UNSET,
) -> Any | DocumentsUpgradeLayoutResponse | None:
    r"""Upgrade dashboard layout

     Upgrades a document to the advanced dashboard layout (the \"File > Upgrade layout\" UI action). No-
    ops when the document already has advanced layout. For published documents the upgrade goes through
    a draft/publish workflow automatically; if a draft already exists, the request returns 409 unless
    `clearExistingDraft` is set to `true`.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpgradeLayoutBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsUpgradeLayoutResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsUpgradeLayoutBody | Unset = UNSET,
) -> Response[Any | DocumentsUpgradeLayoutResponse]:
    r"""Upgrade dashboard layout

     Upgrades a document to the advanced dashboard layout (the \"File > Upgrade layout\" UI action). No-
    ops when the document already has advanced layout. For published documents the upgrade goes through
    a draft/publish workflow automatically; if a draft already exists, the request returns 409 unless
    `clearExistingDraft` is set to `true`.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpgradeLayoutBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsUpgradeLayoutResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsUpgradeLayoutBody | Unset = UNSET,
) -> Any | DocumentsUpgradeLayoutResponse | None:
    r"""Upgrade dashboard layout

     Upgrades a document to the advanced dashboard layout (the \"File > Upgrade layout\" UI action). No-
    ops when the document already has advanced layout. For published documents the upgrade goes through
    a draft/publish workflow automatically; if a draft already exists, the request returns 409 unless
    `clearExistingDraft` is set to `true`.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpgradeLayoutBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsUpgradeLayoutResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
        )
    ).parsed

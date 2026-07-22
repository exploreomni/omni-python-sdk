from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_draft import ApiDraft
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    include: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["include"] = include

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/documents/{identifier}/drafts".format(
            identifier=quote(str(identifier), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | list[ApiDraft] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for componentsschemas_documents_list_drafts_response_item_data in _response_200:
            componentsschemas_documents_list_drafts_response_item = ApiDraft.from_dict(
                componentsschemas_documents_list_drafts_response_item_data
            )

            response_200.append(componentsschemas_documents_list_drafts_response_item)

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | list[ApiDraft]]:
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
    include: str | Unset = UNSET,
) -> Response[Any | list[ApiDraft]]:
    """List document drafts

     Lists drafts for a document with branch context. By default only active drafts are returned; pass
    `include=archived` to also include soft-deleted drafts (retained ~7 days). Results are sorted by
    `createdAt` descending.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        include (str | Unset): Comma-separated list of additional drafts to include. Only
            "archived" is recognized — when present, soft-deleted drafts (retained ~7 days) are
            returned alongside active drafts. Example: archived.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | list[ApiDraft]]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        include=include,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    include: str | Unset = UNSET,
) -> Any | list[ApiDraft] | None:
    """List document drafts

     Lists drafts for a document with branch context. By default only active drafts are returned; pass
    `include=archived` to also include soft-deleted drafts (retained ~7 days). Results are sorted by
    `createdAt` descending.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        include (str | Unset): Comma-separated list of additional drafts to include. Only
            "archived" is recognized — when present, soft-deleted drafts (retained ~7 days) are
            returned alongside active drafts. Example: archived.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | list[ApiDraft]
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        include=include,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    include: str | Unset = UNSET,
) -> Response[Any | list[ApiDraft]]:
    """List document drafts

     Lists drafts for a document with branch context. By default only active drafts are returned; pass
    `include=archived` to also include soft-deleted drafts (retained ~7 days). Results are sorted by
    `createdAt` descending.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        include (str | Unset): Comma-separated list of additional drafts to include. Only
            "archived" is recognized — when present, soft-deleted drafts (retained ~7 days) are
            returned alongside active drafts. Example: archived.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | list[ApiDraft]]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        include=include,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    include: str | Unset = UNSET,
) -> Any | list[ApiDraft] | None:
    """List document drafts

     Lists drafts for a document with branch context. By default only active drafts are returned; pass
    `include=archived` to also include soft-deleted drafts (retained ~7 days). Results are sorted by
    `createdAt` descending.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        include (str | Unset): Comma-separated list of additional drafts to include. Only
            "archived" is recognized — when present, soft-deleted drafts (retained ~7 days) are
            returned alongside active drafts. Example: archived.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | list[ApiDraft]
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            include=include,
        )
    ).parsed

from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_duplicate_body import DocumentsDuplicateBody
from ...models.documents_duplicate_response import DocumentsDuplicateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsDuplicateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/documents/{identifier}/duplicate".format(
            identifier=quote(str(identifier), safe=""),
        ),
        "params": params,
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsDuplicateResponse | None:
    if response.status_code == 201:
        response_201 = DocumentsDuplicateResponse.from_dict(response.json())

        return response_201

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
) -> Response[Any | DocumentsDuplicateResponse]:
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
    body: DocumentsDuplicateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsDuplicateResponse]:
    """Duplicate document

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsDuplicateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsDuplicateResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        body=body,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsDuplicateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsDuplicateResponse | None:
    """Duplicate document

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsDuplicateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsDuplicateResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsDuplicateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsDuplicateResponse]:
    """Duplicate document

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsDuplicateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsDuplicateResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        body=body,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsDuplicateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsDuplicateResponse | None:
    """Duplicate document

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsDuplicateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsDuplicateResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed

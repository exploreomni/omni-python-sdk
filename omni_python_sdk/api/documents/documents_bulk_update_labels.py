from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_bulk_update_labels_body import DocumentsBulkUpdateLabelsBody
from ...models.documents_bulk_update_labels_response import DocumentsBulkUpdateLabelsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsBulkUpdateLabelsBody | Unset = UNSET,
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
        "method": "patch",
        "url": "/api/v1/documents/{identifier}/labels".format(
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
) -> Any | DocumentsBulkUpdateLabelsResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsBulkUpdateLabelsResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsBulkUpdateLabelsResponse]:
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
    body: DocumentsBulkUpdateLabelsBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsBulkUpdateLabelsResponse]:
    """Bulk update document labels

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsBulkUpdateLabelsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsBulkUpdateLabelsResponse]
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
    body: DocumentsBulkUpdateLabelsBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsBulkUpdateLabelsResponse | None:
    """Bulk update document labels

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsBulkUpdateLabelsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsBulkUpdateLabelsResponse
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
    body: DocumentsBulkUpdateLabelsBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | DocumentsBulkUpdateLabelsResponse]:
    """Bulk update document labels

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsBulkUpdateLabelsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsBulkUpdateLabelsResponse]
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
    body: DocumentsBulkUpdateLabelsBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | DocumentsBulkUpdateLabelsResponse | None:
    """Bulk update document labels

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (DocumentsBulkUpdateLabelsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsBulkUpdateLabelsResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed

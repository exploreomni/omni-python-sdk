from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_update_body import DocumentsUpdateBody
from ...models.documents_update_response import DocumentsUpdateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsUpdateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/documents/{identifier}".format(
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
) -> Any | DocumentsUpdateResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsUpdateResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsUpdateResponse]:
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
    body: DocumentsUpdateBody | Unset = UNSET,
) -> Response[Any | DocumentsUpdateResponse]:
    """Rename document

     **Deprecated** — use `PATCH /api/v2/documents/{identifier}/draft` (and the related `/draft` routes).
    Scheduled for removal on July 31, 2026 (see the `Sunset` response header).

    Updates a document's name, description, and/or identifier. This is a partial update — only provided
    fields are modified, and at least one of `name`, `description`, or `identifier` must be supplied.
    When `identifier` is changed, the previous identifier is retained in the document identifier history
    and continues to redirect. For published documents, the update goes through a draft/publish workflow
    automatically.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsUpdateResponse]
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
    body: DocumentsUpdateBody | Unset = UNSET,
) -> Any | DocumentsUpdateResponse | None:
    """Rename document

     **Deprecated** — use `PATCH /api/v2/documents/{identifier}/draft` (and the related `/draft` routes).
    Scheduled for removal on July 31, 2026 (see the `Sunset` response header).

    Updates a document's name, description, and/or identifier. This is a partial update — only provided
    fields are modified, and at least one of `name`, `description`, or `identifier` must be supplied.
    When `identifier` is changed, the previous identifier is retained in the document identifier history
    and continues to redirect. For published documents, the update goes through a draft/publish workflow
    automatically.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsUpdateResponse
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
    body: DocumentsUpdateBody | Unset = UNSET,
) -> Response[Any | DocumentsUpdateResponse]:
    """Rename document

     **Deprecated** — use `PATCH /api/v2/documents/{identifier}/draft` (and the related `/draft` routes).
    Scheduled for removal on July 31, 2026 (see the `Sunset` response header).

    Updates a document's name, description, and/or identifier. This is a partial update — only provided
    fields are modified, and at least one of `name`, `description`, or `identifier` must be supplied.
    When `identifier` is changed, the previous identifier is retained in the document identifier history
    and continues to redirect. For published documents, the update goes through a draft/publish workflow
    automatically.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsUpdateResponse]
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
    body: DocumentsUpdateBody | Unset = UNSET,
) -> Any | DocumentsUpdateResponse | None:
    """Rename document

     **Deprecated** — use `PATCH /api/v2/documents/{identifier}/draft` (and the related `/draft` routes).
    Scheduled for removal on July 31, 2026 (see the `Sunset` response header).

    Updates a document's name, description, and/or identifier. This is a partial update — only provided
    fields are modified, and at least one of `name`, `description`, or `identifier` must be supplied.
    When `identifier` is changed, the previous identifier is retained in the document identifier history
    and continues to redirect. For published documents, the update goes through a draft/publish workflow
    automatically.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.
        body (DocumentsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsUpdateResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
        )
    ).parsed

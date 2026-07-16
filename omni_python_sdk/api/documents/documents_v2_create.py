from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_create_body import DocumentsV2CreateBody
from ...models.documents_v2_create_response import DocumentsV2CreateResponse
from ...types import Response


def _get_kwargs(
    *,
    body: DocumentsV2CreateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v2/documents",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsV2CreateResponse | None:
    if response.status_code == 201:
        response_201 = DocumentsV2CreateResponse.from_dict(response.json())

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

    if response.status_code == 405:
        response_405 = cast(Any, None)
        return response_405

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsV2CreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsV2CreateBody,
) -> Response[Any | DocumentsV2CreateResponse]:
    r"""Create document

     Create a brand-new document and publish it live. Accepts creation metadata (`modelId`, `name`,
    optional `identifier` / `description` / `folderId`) plus the same content slice as the PATCH body —
    `queryPresentations`, `controls`, `settings`, `containers`. The server mints internal tile
    identifiers, so callers omit `miniUuid`. Tiles in `queryPresentations` are merged by key over the
    single empty seed tile at key `\"1\"`; write to `\"1\"` (or send it as `null`) to replace the seed.

    When `containers` is omitted, every dashboard-eligible tile is auto-placed in a default layout. When
    `containers` is present, it fully defines the layout — tiles it does not reference are stored but
    not rendered. Send `containers: null` to create a workbook-only document with no dashboard
    (`controls` and `settings` must then be omitted); an empty `containers: []` is rejected.

    The new document is published live before the response returns. As a first publish of brand-new
    content it is not subject to the org’s `requirePullRequestToPublish` policy (which gates edits to
    existing content).

    Args:
        body (DocumentsV2CreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2CreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsV2CreateBody,
) -> Any | DocumentsV2CreateResponse | None:
    r"""Create document

     Create a brand-new document and publish it live. Accepts creation metadata (`modelId`, `name`,
    optional `identifier` / `description` / `folderId`) plus the same content slice as the PATCH body —
    `queryPresentations`, `controls`, `settings`, `containers`. The server mints internal tile
    identifiers, so callers omit `miniUuid`. Tiles in `queryPresentations` are merged by key over the
    single empty seed tile at key `\"1\"`; write to `\"1\"` (or send it as `null`) to replace the seed.

    When `containers` is omitted, every dashboard-eligible tile is auto-placed in a default layout. When
    `containers` is present, it fully defines the layout — tiles it does not reference are stored but
    not rendered. Send `containers: null` to create a workbook-only document with no dashboard
    (`controls` and `settings` must then be omitted); an empty `containers: []` is rejected.

    The new document is published live before the response returns. As a first publish of brand-new
    content it is not subject to the org’s `requirePullRequestToPublish` policy (which gates edits to
    existing content).

    Args:
        body (DocumentsV2CreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2CreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsV2CreateBody,
) -> Response[Any | DocumentsV2CreateResponse]:
    r"""Create document

     Create a brand-new document and publish it live. Accepts creation metadata (`modelId`, `name`,
    optional `identifier` / `description` / `folderId`) plus the same content slice as the PATCH body —
    `queryPresentations`, `controls`, `settings`, `containers`. The server mints internal tile
    identifiers, so callers omit `miniUuid`. Tiles in `queryPresentations` are merged by key over the
    single empty seed tile at key `\"1\"`; write to `\"1\"` (or send it as `null`) to replace the seed.

    When `containers` is omitted, every dashboard-eligible tile is auto-placed in a default layout. When
    `containers` is present, it fully defines the layout — tiles it does not reference are stored but
    not rendered. Send `containers: null` to create a workbook-only document with no dashboard
    (`controls` and `settings` must then be omitted); an empty `containers: []` is rejected.

    The new document is published live before the response returns. As a first publish of brand-new
    content it is not subject to the org’s `requirePullRequestToPublish` policy (which gates edits to
    existing content).

    Args:
        body (DocumentsV2CreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2CreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: DocumentsV2CreateBody,
) -> Any | DocumentsV2CreateResponse | None:
    r"""Create document

     Create a brand-new document and publish it live. Accepts creation metadata (`modelId`, `name`,
    optional `identifier` / `description` / `folderId`) plus the same content slice as the PATCH body —
    `queryPresentations`, `controls`, `settings`, `containers`. The server mints internal tile
    identifiers, so callers omit `miniUuid`. Tiles in `queryPresentations` are merged by key over the
    single empty seed tile at key `\"1\"`; write to `\"1\"` (or send it as `null`) to replace the seed.

    When `containers` is omitted, every dashboard-eligible tile is auto-placed in a default layout. When
    `containers` is present, it fully defines the layout — tiles it does not reference are stored but
    not rendered. Send `containers: null` to create a workbook-only document with no dashboard
    (`controls` and `settings` must then be omitted); an empty `containers: []` is rejected.

    The new document is published live before the response returns. As a first publish of brand-new
    content it is not subject to the org’s `requirePullRequestToPublish` policy (which gates edits to
    existing content).

    Args:
        body (DocumentsV2CreateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2CreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed

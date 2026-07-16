from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_publish_draft_response import DocumentsV2PublishDraftResponse
from ...types import Response


def _get_kwargs(
    identifier: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v2/documents/{identifier}/draft/publish".format(
            identifier=quote(str(identifier), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsV2PublishDraftResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsV2PublishDraftResponse.from_dict(response.json())

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

    if response.status_code == 405:
        response_405 = cast(Any, None)
        return response_405

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsV2PublishDraftResponse]:
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
) -> Response[Any | DocumentsV2PublishDraftResponse]:
    """Publish draft

     Publish the document's current main (non-branch) draft, promoting it to the published version. No
    request body — the draft is consumed, so the response echoes the now-published document metadata.

    Only the main draft is publishable here; a branch-attached draft is published by merging its branch
    (`POST /api/v1/models/{modelId}/branch/{branchName}/merge`), so a document with no main draft
    returns 404. Documents that require a pull request to publish return 400.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PublishDraftResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DocumentsV2PublishDraftResponse | None:
    """Publish draft

     Publish the document's current main (non-branch) draft, promoting it to the published version. No
    request body — the draft is consumed, so the response echoes the now-published document metadata.

    Only the main draft is publishable here; a branch-attached draft is published by merging its branch
    (`POST /api/v1/models/{modelId}/branch/{branchName}/merge`), so a document with no main draft
    returns 404. Documents that require a pull request to publish return 400.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PublishDraftResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DocumentsV2PublishDraftResponse]:
    """Publish draft

     Publish the document's current main (non-branch) draft, promoting it to the published version. No
    request body — the draft is consumed, so the response echoes the now-published document metadata.

    Only the main draft is publishable here; a branch-attached draft is published by merging its branch
    (`POST /api/v1/models/{modelId}/branch/{branchName}/merge`), so a document with no main draft
    returns 404. Documents that require a pull request to publish return 400.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PublishDraftResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DocumentsV2PublishDraftResponse | None:
    """Publish draft

     Publish the document's current main (non-branch) draft, promoting it to the published version. No
    request body — the draft is consumed, so the response echoes the now-published document metadata.

    Only the main draft is publishable here; a branch-attached draft is published by merging its branch
    (`POST /api/v1/models/{modelId}/branch/{branchName}/merge`), so a document with no main draft
    returns 404. Documents that require a pull request to publish return 400.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PublishDraftResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
        )
    ).parsed

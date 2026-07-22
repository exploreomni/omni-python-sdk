from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_create_draft_body import DocumentsV2CreateDraftBody
from ...models.documents_v2_patch_draft_response import DocumentsV2PatchDraftResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsV2CreateDraftBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v2/documents/{identifier}/draft".format(
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
) -> Any | DocumentsV2PatchDraftResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsV2PatchDraftResponse.from_dict(response.json())

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

    if response.status_code == 422:
        response_422 = cast(Any, None)
        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsV2PatchDraftResponse]:
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
    body: DocumentsV2CreateDraftBody | Unset = UNSET,
) -> Response[Any | DocumentsV2PatchDraftResponse]:
    """Create draft and patch document

     Create a new draft on the published document and apply the patch. No auto-publish — the response
    includes the new `draftIdentifier` for follow-up calls.

    Pass an optional `branchId` to attach the draft to a branch; omit it for a draft on the main
    (unpublished) workspace.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2CreateDraftBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PatchDraftResponse]
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
    body: DocumentsV2CreateDraftBody | Unset = UNSET,
) -> Any | DocumentsV2PatchDraftResponse | None:
    """Create draft and patch document

     Create a new draft on the published document and apply the patch. No auto-publish — the response
    includes the new `draftIdentifier` for follow-up calls.

    Pass an optional `branchId` to attach the draft to a branch; omit it for a draft on the main
    (unpublished) workspace.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2CreateDraftBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PatchDraftResponse
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
    body: DocumentsV2CreateDraftBody | Unset = UNSET,
) -> Response[Any | DocumentsV2PatchDraftResponse]:
    """Create draft and patch document

     Create a new draft on the published document and apply the patch. No auto-publish — the response
    includes the new `draftIdentifier` for follow-up calls.

    Pass an optional `branchId` to attach the draft to a branch; omit it for a draft on the main
    (unpublished) workspace.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2CreateDraftBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PatchDraftResponse]
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
    body: DocumentsV2CreateDraftBody | Unset = UNSET,
) -> Any | DocumentsV2PatchDraftResponse | None:
    """Create draft and patch document

     Create a new draft on the published document and apply the patch. No auto-publish — the response
    includes the new `draftIdentifier` for follow-up calls.

    Pass an optional `branchId` to attach the draft to a branch; omit it for a draft on the main
    (unpublished) workspace.

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2CreateDraftBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PatchDraftResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
        )
    ).parsed

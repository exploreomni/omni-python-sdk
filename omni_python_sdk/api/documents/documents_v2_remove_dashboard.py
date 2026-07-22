from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_patch_draft_response import DocumentsV2PatchDraftResponse
from ...types import Response


def _get_kwargs(
    identifier: str,
    draft_identifier: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v2/documents/{identifier}/draft/{draft_identifier}/dashboard".format(
            identifier=quote(str(identifier), safe=""),
            draft_identifier=quote(str(draft_identifier), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsV2PatchDraftResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsV2PatchDraftResponse.from_dict(response.json())

        return response_200

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
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DocumentsV2PatchDraftResponse]:
    r"""Remove dashboard from document

     Remove the dashboard from an existing draft, leaving a workbook-only document. Its schedules are
    removed too, but at publish time (see below), not on this call. Parity with the UI’s \"Remove
    dashboard\" action, and the inverse of adding a dashboard via a `containers` patch.

    Operates only on the draft named by `draftIdentifier`, which the caller creates first via `PATCH
    …/draft`. Requiring an explicit draft keeps the removal from silently reusing (and clobbering) a
    draft that holds other unpublished work.

    No auto-publish — publish via `POST …/draft/publish` to make the document workbook-only (publishing
    also clears the previously-published dashboard’s schedules). Idempotent: a draft that is already
    workbook-only returns 200 unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PatchDraftResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        draft_identifier=draft_identifier,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    identifier: str,
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DocumentsV2PatchDraftResponse | None:
    r"""Remove dashboard from document

     Remove the dashboard from an existing draft, leaving a workbook-only document. Its schedules are
    removed too, but at publish time (see below), not on this call. Parity with the UI’s \"Remove
    dashboard\" action, and the inverse of adding a dashboard via a `containers` patch.

    Operates only on the draft named by `draftIdentifier`, which the caller creates first via `PATCH
    …/draft`. Requiring an explicit draft keeps the removal from silently reusing (and clobbering) a
    draft that holds other unpublished work.

    No auto-publish — publish via `POST …/draft/publish` to make the document workbook-only (publishing
    also clears the previously-published dashboard’s schedules). Idempotent: a draft that is already
    workbook-only returns 200 unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PatchDraftResponse
    """

    return sync_detailed(
        identifier=identifier,
        draft_identifier=draft_identifier,
        client=client,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DocumentsV2PatchDraftResponse]:
    r"""Remove dashboard from document

     Remove the dashboard from an existing draft, leaving a workbook-only document. Its schedules are
    removed too, but at publish time (see below), not on this call. Parity with the UI’s \"Remove
    dashboard\" action, and the inverse of adding a dashboard via a `containers` patch.

    Operates only on the draft named by `draftIdentifier`, which the caller creates first via `PATCH
    …/draft`. Requiring an explicit draft keeps the removal from silently reusing (and clobbering) a
    draft that holds other unpublished work.

    No auto-publish — publish via `POST …/draft/publish` to make the document workbook-only (publishing
    also clears the previously-published dashboard’s schedules). Idempotent: a draft that is already
    workbook-only returns 200 unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2PatchDraftResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        draft_identifier=draft_identifier,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Any | DocumentsV2PatchDraftResponse | None:
    r"""Remove dashboard from document

     Remove the dashboard from an existing draft, leaving a workbook-only document. Its schedules are
    removed too, but at publish time (see below), not on this call. Parity with the UI’s \"Remove
    dashboard\" action, and the inverse of adding a dashboard via a `containers` patch.

    Operates only on the draft named by `draftIdentifier`, which the caller creates first via `PATCH
    …/draft`. Requiring an explicit draft keeps the removal from silently reusing (and clobbering) a
    draft that holds other unpublished work.

    No auto-publish — publish via `POST …/draft/publish` to make the document workbook-only (publishing
    also clears the previously-published dashboard’s schedules). Idempotent: a draft that is already
    workbook-only returns 200 unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2PatchDraftResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            draft_identifier=draft_identifier,
            client=client,
        )
    ).parsed

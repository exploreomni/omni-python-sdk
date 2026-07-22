from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_update_identifier_body import DocumentsV2UpdateIdentifierBody
from ...models.documents_v2_update_identifier_response import DocumentsV2UpdateIdentifierResponse
from ...types import Response


def _get_kwargs(
    identifier: str,
    *,
    body: DocumentsV2UpdateIdentifierBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/v2/documents/{identifier}/identifier".format(
            identifier=quote(str(identifier), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsV2UpdateIdentifierResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsV2UpdateIdentifierResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsV2UpdateIdentifierResponse]:
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
    body: DocumentsV2UpdateIdentifierBody,
) -> Response[Any | DocumentsV2UpdateIdentifierResponse]:
    """Rename document identifier

     Rename a published document's identifier. The change is applied live and immediately — it does not
    go through the draft/publish workflow — and the former identifier is recorded in the document's
    rename history.

    Only published documents can be renamed. A draft target returns 409; an unknown or archived target
    returns 404. The new identifier must be a valid slug (otherwise 400) and unused by any other
    document in the organization (otherwise 409).

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2UpdateIdentifierBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2UpdateIdentifierResponse]
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
    body: DocumentsV2UpdateIdentifierBody,
) -> Any | DocumentsV2UpdateIdentifierResponse | None:
    """Rename document identifier

     Rename a published document's identifier. The change is applied live and immediately — it does not
    go through the draft/publish workflow — and the former identifier is recorded in the document's
    rename history.

    Only published documents can be renamed. A draft target returns 409; an unknown or archived target
    returns 404. The new identifier must be a valid slug (otherwise 400) and unused by any other
    document in the organization (otherwise 409).

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2UpdateIdentifierBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2UpdateIdentifierResponse
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
    body: DocumentsV2UpdateIdentifierBody,
) -> Response[Any | DocumentsV2UpdateIdentifierResponse]:
    """Rename document identifier

     Rename a published document's identifier. The change is applied live and immediately — it does not
    go through the draft/publish workflow — and the former identifier is recorded in the document's
    rename history.

    Only published documents can be renamed. A draft target returns 409; an unknown or archived target
    returns 404. The new identifier must be a valid slug (otherwise 400) and unused by any other
    document in the organization (otherwise 409).

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2UpdateIdentifierBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2UpdateIdentifierResponse]
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
    body: DocumentsV2UpdateIdentifierBody,
) -> Any | DocumentsV2UpdateIdentifierResponse | None:
    """Rename document identifier

     Rename a published document's identifier. The change is applied live and immediately — it does not
    go through the draft/publish workflow — and the former identifier is recorded in the document's
    rename history.

    Only published documents can be renamed. A draft target returns 409; an unknown or archived target
    returns 404. The new identifier must be a valid slug (otherwise 400) and unused by any other
    document in the organization (otherwise 409).

    Args:
        identifier (str): Document identifier — either the URL slug (e.g. `abc123`) or the
            canonical workbook UUID. Example: abc123.
        body (DocumentsV2UpdateIdentifierBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2UpdateIdentifierResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
            body=body,
        )
    ).parsed

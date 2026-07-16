from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_get_response import DocumentsGetResponse
from ...types import Response


def _get_kwargs(
    identifier: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/documents/{identifier}".format(
            identifier=quote(str(identifier), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsGetResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsGetResponse.from_dict(response.json())

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
) -> Response[Any | DocumentsGetResponse]:
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
) -> Response[Any | DocumentsGetResponse]:
    """Get document

     Retrieves a document's configuration in a format compatible with PUT for round-trip editing. GET a
    document, modify the response, and PUT it back to update. Only dashboard documents are supported;
    analysis documents return 400.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsGetResponse]
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
) -> Any | DocumentsGetResponse | None:
    """Get document

     Retrieves a document's configuration in a format compatible with PUT for round-trip editing. GET a
    document, modify the response, and PUT it back to update. Only dashboard documents are supported;
    analysis documents return 400.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsGetResponse
    """

    return sync_detailed(
        identifier=identifier,
        client=client,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | DocumentsGetResponse]:
    """Get document

     Retrieves a document's configuration in a format compatible with PUT for round-trip editing. GET a
    document, modify the response, and PUT it back to update. Only dashboard documents are supported;
    analysis documents return 400.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsGetResponse]
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
) -> Any | DocumentsGetResponse | None:
    """Get document

     Retrieves a document's configuration in a format compatible with PUT for round-trip editing. GET a
    document, modify the response, and PUT it back to update. Only dashboard documents are supported;
    analysis documents return 400.

    Args:
        identifier (str): Document identifier (either document ID or identifier slug) Example:
            abc123.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsGetResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            client=client,
        )
    ).parsed

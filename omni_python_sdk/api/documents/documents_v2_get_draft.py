from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.documents_v2_get_draft_pretty import DocumentsV2GetDraftPretty
from ...models.documents_v2_read_response import DocumentsV2ReadResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    identifier: str,
    draft_identifier: str,
    *,
    pretty: DocumentsV2GetDraftPretty | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_pretty: str | Unset = UNSET
    if not isinstance(pretty, Unset):
        json_pretty = pretty

    params["pretty"] = json_pretty

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v2/documents/{identifier}/draft/{draft_identifier}".format(
            identifier=quote(str(identifier), safe=""),
            draft_identifier=quote(str(draft_identifier), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | DocumentsV2ReadResponse | None:
    if response.status_code == 200:
        response_200 = DocumentsV2ReadResponse.from_dict(response.json())

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

    if response.status_code == 422:
        response_422 = cast(Any, None)
        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | DocumentsV2ReadResponse]:
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
    pretty: DocumentsV2GetDraftPretty | Unset = UNSET,
) -> Response[Any | DocumentsV2ReadResponse]:
    r"""Read draft state

     Read the named draft's state. Returns the full `DocumentsV2ReadResponse` shape — same as the live-
    state read endpoint.

    The response is structured so a caller can take it verbatim and submit it as the body of the draft
    PATCH routes. Tiles in `queryPresentations.data` are keyed by a stable record key (e.g. `\"1\"`,
    `\"2\"`) — the server uses that key to identify existing tiles for updates, so callers do not need
    to track or send any other identifier. Control IDs and container `instanceKey` / `referenceKey`
    values also round-trip unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.
        pretty (DocumentsV2GetDraftPretty | Unset): Set `true` or `1` to pretty-print (2-space
            indent) the response; `false` / `0` (the default) is compact. Key ordering is
            deterministic regardless.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2ReadResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        draft_identifier=draft_identifier,
        pretty=pretty,
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
    pretty: DocumentsV2GetDraftPretty | Unset = UNSET,
) -> Any | DocumentsV2ReadResponse | None:
    r"""Read draft state

     Read the named draft's state. Returns the full `DocumentsV2ReadResponse` shape — same as the live-
    state read endpoint.

    The response is structured so a caller can take it verbatim and submit it as the body of the draft
    PATCH routes. Tiles in `queryPresentations.data` are keyed by a stable record key (e.g. `\"1\"`,
    `\"2\"`) — the server uses that key to identify existing tiles for updates, so callers do not need
    to track or send any other identifier. Control IDs and container `instanceKey` / `referenceKey`
    values also round-trip unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.
        pretty (DocumentsV2GetDraftPretty | Unset): Set `true` or `1` to pretty-print (2-space
            indent) the response; `false` / `0` (the default) is compact. Key ordering is
            deterministic regardless.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2ReadResponse
    """

    return sync_detailed(
        identifier=identifier,
        draft_identifier=draft_identifier,
        client=client,
        pretty=pretty,
    ).parsed


async def asyncio_detailed(
    identifier: str,
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
    pretty: DocumentsV2GetDraftPretty | Unset = UNSET,
) -> Response[Any | DocumentsV2ReadResponse]:
    r"""Read draft state

     Read the named draft's state. Returns the full `DocumentsV2ReadResponse` shape — same as the live-
    state read endpoint.

    The response is structured so a caller can take it verbatim and submit it as the body of the draft
    PATCH routes. Tiles in `queryPresentations.data` are keyed by a stable record key (e.g. `\"1\"`,
    `\"2\"`) — the server uses that key to identify existing tiles for updates, so callers do not need
    to track or send any other identifier. Control IDs and container `instanceKey` / `referenceKey`
    values also round-trip unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.
        pretty (DocumentsV2GetDraftPretty | Unset): Set `true` or `1` to pretty-print (2-space
            indent) the response; `false` / `0` (the default) is compact. Key ordering is
            deterministic regardless.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | DocumentsV2ReadResponse]
    """

    kwargs = _get_kwargs(
        identifier=identifier,
        draft_identifier=draft_identifier,
        pretty=pretty,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    identifier: str,
    draft_identifier: str,
    *,
    client: AuthenticatedClient | Client,
    pretty: DocumentsV2GetDraftPretty | Unset = UNSET,
) -> Any | DocumentsV2ReadResponse | None:
    r"""Read draft state

     Read the named draft's state. Returns the full `DocumentsV2ReadResponse` shape — same as the live-
    state read endpoint.

    The response is structured so a caller can take it verbatim and submit it as the body of the draft
    PATCH routes. Tiles in `queryPresentations.data` are keyed by a stable record key (e.g. `\"1\"`,
    `\"2\"`) — the server uses that key to identify existing tiles for updates, so callers do not need
    to track or send any other identifier. Control IDs and container `instanceKey` / `referenceKey`
    values also round-trip unchanged.

    Args:
        identifier (str): Published document identifier. Example: abc123.
        draft_identifier (str): Draft workbook identifier (see `PATCH
            /api/v2/documents/{identifier}/draft`). Example: def456.
        pretty (DocumentsV2GetDraftPretty | Unset): Set `true` or `1` to pretty-print (2-space
            indent) the response; `false` / `0` (the default) is compact. Key ordering is
            deterministic regardless.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | DocumentsV2ReadResponse
    """

    return (
        await asyncio_detailed(
            identifier=identifier,
            draft_identifier=draft_identifier,
            client=client,
            pretty=pretty,
        )
    ).parsed

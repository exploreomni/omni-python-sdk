from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.whoami_response import WhoamiResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    model_id: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["modelId"] = model_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/whoami",
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | WhoamiResponse | None:
    if response.status_code == 200:
        response_200 = WhoamiResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | WhoamiResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    model_id: str | Unset = UNSET,
) -> Response[Any | WhoamiResponse]:
    """Get current identity and permissions (whoami)

     Returns the authenticated caller's own identity, API key scope, organization role, and resolved per-
    model permissions. Self-scoped and available to non-admins: it lets a caller decide whether an
    action is permitted without attempting it. Pass `modelId` to scope `rolesByModel` to specific
    models.

    Args:
        model_id (str | Unset): Optional model filter. A single model id or a comma-separated
            list. When provided, `rolesByModel` contains only these models. When omitted, models the
            caller can access are returned (up to a limit; see `rolesByModelTruncated`). Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | WhoamiResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    model_id: str | Unset = UNSET,
) -> Any | WhoamiResponse | None:
    """Get current identity and permissions (whoami)

     Returns the authenticated caller's own identity, API key scope, organization role, and resolved per-
    model permissions. Self-scoped and available to non-admins: it lets a caller decide whether an
    action is permitted without attempting it. Pass `modelId` to scope `rolesByModel` to specific
    models.

    Args:
        model_id (str | Unset): Optional model filter. A single model id or a comma-separated
            list. When provided, `rolesByModel` contains only these models. When omitted, models the
            caller can access are returned (up to a limit; see `rolesByModelTruncated`). Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | WhoamiResponse
    """

    return sync_detailed(
        client=client,
        model_id=model_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    model_id: str | Unset = UNSET,
) -> Response[Any | WhoamiResponse]:
    """Get current identity and permissions (whoami)

     Returns the authenticated caller's own identity, API key scope, organization role, and resolved per-
    model permissions. Self-scoped and available to non-admins: it lets a caller decide whether an
    action is permitted without attempting it. Pass `modelId` to scope `rolesByModel` to specific
    models.

    Args:
        model_id (str | Unset): Optional model filter. A single model id or a comma-separated
            list. When provided, `rolesByModel` contains only these models. When omitted, models the
            caller can access are returned (up to a limit; see `rolesByModelTruncated`). Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | WhoamiResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    model_id: str | Unset = UNSET,
) -> Any | WhoamiResponse | None:
    """Get current identity and permissions (whoami)

     Returns the authenticated caller's own identity, API key scope, organization role, and resolved per-
    model permissions. Self-scoped and available to non-admins: it lets a caller decide whether an
    action is permitted without attempting it. Pass `modelId` to scope `rolesByModel` to specific
    models.

    Args:
        model_id (str | Unset): Optional model filter. A single model id or a comma-separated
            list. When provided, `rolesByModel` contains only these models. When omitted, models the
            caller can access are returned (up to a limit; see `rolesByModelTruncated`). Example:
            550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | WhoamiResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            model_id=model_id,
        )
    ).parsed

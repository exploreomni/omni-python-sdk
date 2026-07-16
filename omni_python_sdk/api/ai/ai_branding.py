from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_branding_response import AiBrandingResponse
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/ai/branding",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiBrandingResponse | ApiError401 | ApiError403 | None:
    if response.status_code == 200:
        response_200 = AiBrandingResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = ApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ApiError403.from_dict(response.json())

        return response_403

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiBrandingResponse | ApiError401 | ApiError403]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiBrandingResponse | ApiError401 | ApiError403]:
    """Get AI helper branding

     Returns the organization's AI helper branding — display name, optional custom logo URL, and copy
    used on AI helper landing surfaces (headline, body, prompt placeholder). Falls back to Omni's
    defaults when the organization hasn't configured custom branding, so the response is always
    populated. Used by client apps (iOS, embeds) to render the AI helper with the org's chosen identity.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiBrandingResponse | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> AiBrandingResponse | ApiError401 | ApiError403 | None:
    """Get AI helper branding

     Returns the organization's AI helper branding — display name, optional custom logo URL, and copy
    used on AI helper landing surfaces (headline, body, prompt placeholder). Falls back to Omni's
    defaults when the organization hasn't configured custom branding, so the response is always
    populated. Used by client apps (iOS, embeds) to render the AI helper with the org's chosen identity.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiBrandingResponse | ApiError401 | ApiError403
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[AiBrandingResponse | ApiError401 | ApiError403]:
    """Get AI helper branding

     Returns the organization's AI helper branding — display name, optional custom logo URL, and copy
    used on AI helper landing surfaces (headline, body, prompt placeholder). Falls back to Omni's
    defaults when the organization hasn't configured custom branding, so the response is always
    populated. Used by client apps (iOS, embeds) to render the AI helper with the org's chosen identity.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiBrandingResponse | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> AiBrandingResponse | ApiError401 | ApiError403 | None:
    """Get AI helper branding

     Returns the organization's AI helper branding — display name, optional custom logo URL, and copy
    used on AI helper landing surfaces (headline, body, prompt placeholder). Falls back to Omni's
    defaults when the organization hasn't configured custom branding, so the response is always
    populated. Used by client apps (iOS, embeds) to render the AI helper with the org's chosen identity.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiBrandingResponse | ApiError401 | ApiError403
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed

from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.success_response import SuccessResponse
from ...types import Response


def _get_kwargs(
    model_id: UUID,
    suggestion_id: UUID,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models/{model_id}/suggestions/{suggestion_id}/restore".format(
            model_id=quote(str(model_id), safe=""),
            suggestion_id=quote(str(suggestion_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | SuccessResponse | None:
    if response.status_code == 200:
        response_200 = SuccessResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | SuccessResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    suggestion_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | SuccessResponse]:
    """Restore a suggestion

     Restores a previously dismissed suggestion back to the active list. Requires organization admin
    permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestion belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        suggestion_id (UUID): UUID of the suggestion Example:
            b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        suggestion_id=suggestion_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    suggestion_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | SuccessResponse | None:
    """Restore a suggestion

     Restores a previously dismissed suggestion back to the active list. Requires organization admin
    permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestion belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        suggestion_id (UUID): UUID of the suggestion Example:
            b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return sync_detailed(
        model_id=model_id,
        suggestion_id=suggestion_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    suggestion_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[Any | SuccessResponse]:
    """Restore a suggestion

     Restores a previously dismissed suggestion back to the active list. Requires organization admin
    permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestion belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        suggestion_id (UUID): UUID of the suggestion Example:
            b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        suggestion_id=suggestion_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    suggestion_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Any | SuccessResponse | None:
    """Restore a suggestion

     Restores a previously dismissed suggestion back to the active list. Requires organization admin
    permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestion belongs to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        suggestion_id (UUID): UUID of the suggestion Example:
            b2c3d4e5-f6a7-8901-bcde-f12345678901.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            suggestion_id=suggestion_id,
            client=client,
        )
    ).parsed

from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.schedule_suggestions_body import ScheduleSuggestionsBody
from ...models.schedule_suggestions_response import ScheduleSuggestionsResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    body: ScheduleSuggestionsBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/v1/models/{model_id}/suggestions/schedule".format(
            model_id=quote(str(model_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ScheduleSuggestionsResponse | None:
    if response.status_code == 200:
        response_200 = ScheduleSuggestionsResponse.from_dict(response.json())

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
) -> Response[Any | ScheduleSuggestionsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ScheduleSuggestionsBody | Unset = UNSET,
) -> Response[Any | ScheduleSuggestionsResponse]:
    """Enable the suggestion schedule

     Enables the daily schedule that generates suggestions for the shared model. Idempotent — re-enabling
    leaves an existing schedule untouched. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        body (ScheduleSuggestionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScheduleSuggestionsResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ScheduleSuggestionsBody | Unset = UNSET,
) -> Any | ScheduleSuggestionsResponse | None:
    """Enable the suggestion schedule

     Enables the daily schedule that generates suggestions for the shared model. Idempotent — re-enabling
    leaves an existing schedule untouched. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        body (ScheduleSuggestionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScheduleSuggestionsResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ScheduleSuggestionsBody | Unset = UNSET,
) -> Response[Any | ScheduleSuggestionsResponse]:
    """Enable the suggestion schedule

     Enables the daily schedule that generates suggestions for the shared model. Idempotent — re-enabling
    leaves an existing schedule untouched. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        body (ScheduleSuggestionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScheduleSuggestionsResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ScheduleSuggestionsBody | Unset = UNSET,
) -> Any | ScheduleSuggestionsResponse | None:
    """Enable the suggestion schedule

     Enables the daily schedule that generates suggestions for the shared model. Idempotent — re-enabling
    leaves an existing schedule untouched. Requires organization admin permissions.

    Args:
        model_id (UUID): UUID of the shared model the suggestions belong to Example:
            a1b2c3d4-e5f6-7890-abcd-ef1234567890.
        body (ScheduleSuggestionsBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScheduleSuggestionsResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            body=body,
        )
    ).parsed

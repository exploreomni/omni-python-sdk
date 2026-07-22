from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.labels_update_body import LabelsUpdateBody
from ...models.labels_update_response import LabelsUpdateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    name: str,
    *,
    body: LabelsUpdateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_user_id: str | Unset = UNSET
    if not isinstance(user_id, Unset):
        json_user_id = str(user_id)
    params["userId"] = json_user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/v1/labels/{name}".format(
            name=quote(str(name), safe=""),
        ),
        "params": params,
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | LabelsUpdateResponse | None:
    if response.status_code == 200:
        response_200 = LabelsUpdateResponse.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | LabelsUpdateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    name: str,
    *,
    client: AuthenticatedClient | Client,
    body: LabelsUpdateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | LabelsUpdateResponse]:
    """Update a label

    Args:
        name (str): Label name Example: verified.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (LabelsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | LabelsUpdateResponse]
    """

    kwargs = _get_kwargs(
        name=name,
        body=body,
        user_id=user_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    name: str,
    *,
    client: AuthenticatedClient | Client,
    body: LabelsUpdateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | LabelsUpdateResponse | None:
    """Update a label

    Args:
        name (str): Label name Example: verified.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (LabelsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | LabelsUpdateResponse
    """

    return sync_detailed(
        name=name,
        client=client,
        body=body,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    name: str,
    *,
    client: AuthenticatedClient | Client,
    body: LabelsUpdateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Response[Any | LabelsUpdateResponse]:
    """Update a label

    Args:
        name (str): Label name Example: verified.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (LabelsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | LabelsUpdateResponse]
    """

    kwargs = _get_kwargs(
        name=name,
        body=body,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    name: str,
    *,
    client: AuthenticatedClient | Client,
    body: LabelsUpdateBody | Unset = UNSET,
    user_id: UUID | Unset = UNSET,
) -> Any | LabelsUpdateResponse | None:
    """Update a label

    Args:
        name (str): Label name Example: verified.
        user_id (UUID | Unset): Target user membership ID (for org-scoped API keys)
        body (LabelsUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | LabelsUpdateResponse
    """

    return (
        await asyncio_detailed(
            name=name,
            client=client,
            body=body,
            user_id=user_id,
        )
    ).parsed

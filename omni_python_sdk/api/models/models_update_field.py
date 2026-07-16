from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_update_field_body import ModelsUpdateFieldBody
from ...models.success_response import SuccessResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    view_name: str,
    field_name: str,
    *,
    body: ModelsUpdateFieldBody | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/models/{model_id}/view/{view_name}/field/{field_name}".format(
            model_id=quote(str(model_id), safe=""),
            view_name=quote(str(view_name), safe=""),
            field_name=quote(str(field_name), safe=""),
        ),
        "params": params,
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
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
    view_name: str,
    field_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsUpdateFieldBody | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | SuccessResponse]:
    """Update field

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        view_name (str): View name Example: orders.
        field_name (str): Field name Example: total_amount.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        body (ModelsUpdateFieldBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        view_name=view_name,
        field_name=field_name,
        body=body,
        branch_id=branch_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    view_name: str,
    field_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsUpdateFieldBody | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Any | SuccessResponse | None:
    """Update field

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        view_name (str): View name Example: orders.
        field_name (str): Field name Example: total_amount.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        body (ModelsUpdateFieldBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return sync_detailed(
        model_id=model_id,
        view_name=view_name,
        field_name=field_name,
        client=client,
        body=body,
        branch_id=branch_id,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    view_name: str,
    field_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsUpdateFieldBody | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Response[Any | SuccessResponse]:
    """Update field

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        view_name (str): View name Example: orders.
        field_name (str): Field name Example: total_amount.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        body (ModelsUpdateFieldBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        view_name=view_name,
        field_name=field_name,
        body=body,
        branch_id=branch_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    view_name: str,
    field_name: str,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsUpdateFieldBody | Unset = UNSET,
    branch_id: UUID | Unset = UNSET,
) -> Any | SuccessResponse | None:
    """Update field

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        view_name (str): View name Example: orders.
        field_name (str): Field name Example: total_amount.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        body (ModelsUpdateFieldBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            view_name=view_name,
            field_name=field_name,
            client=client,
            body=body,
            branch_id=branch_id,
        )
    ).parsed

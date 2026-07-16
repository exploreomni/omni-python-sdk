from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_delete_topic_mode import ModelsDeleteTopicMode
from ...models.success_response import SuccessResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    topic_name: str,
    *,
    branch_id: UUID | Unset = UNSET,
    mode: ModelsDeleteTopicMode | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    json_mode: str | Unset = UNSET
    if not isinstance(mode, Unset):
        json_mode = mode

    params["mode"] = json_mode

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/api/v1/models/{model_id}/topic/{topic_name}".format(
            model_id=quote(str(model_id), safe=""),
            topic_name=quote(str(topic_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | SuccessResponse | None:
    if response.status_code == 200:
        response_200 = SuccessResponse.from_dict(response.json())

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
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    mode: ModelsDeleteTopicMode | Unset = UNSET,
) -> Response[Any | SuccessResponse]:
    """Delete topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        mode (ModelsDeleteTopicMode | Unset): Controls delete behavior to match IDE editing modes.
            COMBINED (default) adds the topic to deletedTopics if it exists in the parent model
            (prevents reappearing). MERGED behaves the same as COMBINED. EXTENSION hard-deletes the
            topic from the extension layer. Example: COMBINED.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        topic_name=topic_name,
        branch_id=branch_id,
        mode=mode,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    mode: ModelsDeleteTopicMode | Unset = UNSET,
) -> Any | SuccessResponse | None:
    """Delete topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        mode (ModelsDeleteTopicMode | Unset): Controls delete behavior to match IDE editing modes.
            COMBINED (default) adds the topic to deletedTopics if it exists in the parent model
            (prevents reappearing). MERGED behaves the same as COMBINED. EXTENSION hard-deletes the
            topic from the extension layer. Example: COMBINED.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return sync_detailed(
        model_id=model_id,
        topic_name=topic_name,
        client=client,
        branch_id=branch_id,
        mode=mode,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    mode: ModelsDeleteTopicMode | Unset = UNSET,
) -> Response[Any | SuccessResponse]:
    """Delete topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        mode (ModelsDeleteTopicMode | Unset): Controls delete behavior to match IDE editing modes.
            COMBINED (default) adds the topic to deletedTopics if it exists in the parent model
            (prevents reappearing). MERGED behaves the same as COMBINED. EXTENSION hard-deletes the
            topic from the extension layer. Example: COMBINED.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SuccessResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        topic_name=topic_name,
        branch_id=branch_id,
        mode=mode,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    topic_name: str,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    mode: ModelsDeleteTopicMode | Unset = UNSET,
) -> Any | SuccessResponse | None:
    """Delete topic

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        topic_name (str): Topic name Example: sales_analytics.
        branch_id (UUID | Unset): Branch ID to use for branch-aware operations Example:
            123e4567-e89b-12d3-a456-426614174001.
        mode (ModelsDeleteTopicMode | Unset): Controls delete behavior to match IDE editing modes.
            COMBINED (default) adds the topic to deletedTopics if it exists in the parent model
            (prevents reappearing). MERGED behaves the same as COMBINED. EXTENSION hard-deletes the
            topic from the extension layer. Example: COMBINED.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SuccessResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            topic_name=topic_name,
            client=client,
            branch_id=branch_id,
            mode=mode,
        )
    ).parsed

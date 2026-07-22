from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_model_schema_base import CreateModelSchemaBase
from ...models.models_create_models_create_response import ModelsCreateModelsCreateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: CreateModelSchemaBase | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/models",
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsCreateModelsCreateResponse | None:
    if response.status_code == 200:
        response_200 = ModelsCreateModelsCreateResponse.from_dict(response.json())

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
) -> Response[Any | ModelsCreateModelsCreateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: CreateModelSchemaBase | Unset = UNSET,
) -> Response[Any | ModelsCreateModelsCreateResponse]:
    """Create model

     Create a new model. Supports creating schema, shared, branch, shared_extension, and query models. A
    query model (modelKind QUERY) is created empty under a workbook model (baseModelId); populate its
    views and fields via the model YAML endpoint.

    Args:
        body (CreateModelSchemaBase | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCreateModelsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: CreateModelSchemaBase | Unset = UNSET,
) -> Any | ModelsCreateModelsCreateResponse | None:
    """Create model

     Create a new model. Supports creating schema, shared, branch, shared_extension, and query models. A
    query model (modelKind QUERY) is created empty under a workbook model (baseModelId); populate its
    views and fields via the model YAML endpoint.

    Args:
        body (CreateModelSchemaBase | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCreateModelsCreateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: CreateModelSchemaBase | Unset = UNSET,
) -> Response[Any | ModelsCreateModelsCreateResponse]:
    """Create model

     Create a new model. Supports creating schema, shared, branch, shared_extension, and query models. A
    query model (modelKind QUERY) is created empty under a workbook model (baseModelId); populate its
    views and fields via the model YAML endpoint.

    Args:
        body (CreateModelSchemaBase | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsCreateModelsCreateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: CreateModelSchemaBase | Unset = UNSET,
) -> Any | ModelsCreateModelsCreateResponse | None:
    """Create model

     Create a new model. Supports creating schema, shared, branch, shared_extension, and query models. A
    query model (modelKind QUERY) is created empty under a workbook model (baseModelId); populate its
    views and fields via the model YAML endpoint.

    Args:
        body (CreateModelSchemaBase | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsCreateModelsCreateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed

from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.model_yaml_response import ModelYamlResponse
from ...models.models_yaml_get_mode import ModelsYamlGetMode
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    branch_id: UUID | Unset = UNSET,
    file_name: str | Unset = UNSET,
    mode: ModelsYamlGetMode | Unset = "combined",
    fully_resolved: bool | str | Unset = "False",
    include_checksums: bool | None | Unset = False,
    include_schemas: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branchId"] = json_branch_id

    params["fileName"] = file_name

    json_mode: str | Unset = UNSET
    if not isinstance(mode, Unset):
        json_mode = mode

    params["mode"] = json_mode

    json_fully_resolved: bool | str | Unset
    if isinstance(fully_resolved, Unset):
        json_fully_resolved = UNSET
    else:
        json_fully_resolved = fully_resolved
    params["fullyResolved"] = json_fully_resolved

    json_include_checksums: bool | None | Unset
    if isinstance(include_checksums, Unset):
        json_include_checksums = UNSET
    else:
        json_include_checksums = include_checksums
    params["includeChecksums"] = json_include_checksums

    params["includeSchemas"] = include_schemas

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/yaml".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelYamlResponse | None:
    if response.status_code == 200:
        response_200 = ModelYamlResponse.from_dict(response.json())

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
) -> Response[Any | ModelYamlResponse]:
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
    branch_id: UUID | Unset = UNSET,
    file_name: str | Unset = UNSET,
    mode: ModelsYamlGetMode | Unset = "combined",
    fully_resolved: bool | str | Unset = "False",
    include_checksums: bool | None | Unset = False,
    include_schemas: str | Unset = UNSET,
) -> Response[Any | ModelYamlResponse]:
    """Get model YAML

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str | Unset): File name to operate on
        mode (ModelsYamlGetMode | Unset): IDE mode for YAML operations Default: 'combined'.
        fully_resolved (bool | str | Unset): Resolve the model extends chain so the returned YAML
            reflects what runs at query time. Only valid with mode=combined. Default: 'False'.
        include_checksums (bool | None | Unset): Include checksums in response Default: False.
        include_schemas (str | Unset): A single schema name (optionally catalog-scoped, e.g.
            'warehouse.reporting') to additionally load into the response. Use this to include view
            YAML from a schema that isn't active in the model (inactive or offloaded). Only views from
            this schema will be returned (views with no schema are always included).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelYamlResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        file_name=file_name,
        mode=mode,
        fully_resolved=fully_resolved,
        include_checksums=include_checksums,
        include_schemas=include_schemas,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    file_name: str | Unset = UNSET,
    mode: ModelsYamlGetMode | Unset = "combined",
    fully_resolved: bool | str | Unset = "False",
    include_checksums: bool | None | Unset = False,
    include_schemas: str | Unset = UNSET,
) -> Any | ModelYamlResponse | None:
    """Get model YAML

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str | Unset): File name to operate on
        mode (ModelsYamlGetMode | Unset): IDE mode for YAML operations Default: 'combined'.
        fully_resolved (bool | str | Unset): Resolve the model extends chain so the returned YAML
            reflects what runs at query time. Only valid with mode=combined. Default: 'False'.
        include_checksums (bool | None | Unset): Include checksums in response Default: False.
        include_schemas (str | Unset): A single schema name (optionally catalog-scoped, e.g.
            'warehouse.reporting') to additionally load into the response. Use this to include view
            YAML from a schema that isn't active in the model (inactive or offloaded). Only views from
            this schema will be returned (views with no schema are always included).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelYamlResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        branch_id=branch_id,
        file_name=file_name,
        mode=mode,
        fully_resolved=fully_resolved,
        include_checksums=include_checksums,
        include_schemas=include_schemas,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    file_name: str | Unset = UNSET,
    mode: ModelsYamlGetMode | Unset = "combined",
    fully_resolved: bool | str | Unset = "False",
    include_checksums: bool | None | Unset = False,
    include_schemas: str | Unset = UNSET,
) -> Response[Any | ModelYamlResponse]:
    """Get model YAML

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str | Unset): File name to operate on
        mode (ModelsYamlGetMode | Unset): IDE mode for YAML operations Default: 'combined'.
        fully_resolved (bool | str | Unset): Resolve the model extends chain so the returned YAML
            reflects what runs at query time. Only valid with mode=combined. Default: 'False'.
        include_checksums (bool | None | Unset): Include checksums in response Default: False.
        include_schemas (str | Unset): A single schema name (optionally catalog-scoped, e.g.
            'warehouse.reporting') to additionally load into the response. Use this to include view
            YAML from a schema that isn't active in the model (inactive or offloaded). Only views from
            this schema will be returned (views with no schema are always included).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelYamlResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        file_name=file_name,
        mode=mode,
        fully_resolved=fully_resolved,
        include_checksums=include_checksums,
        include_schemas=include_schemas,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    file_name: str | Unset = UNSET,
    mode: ModelsYamlGetMode | Unset = "combined",
    fully_resolved: bool | str | Unset = "False",
    include_checksums: bool | None | Unset = False,
    include_schemas: str | Unset = UNSET,
) -> Any | ModelYamlResponse | None:
    """Get model YAML

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Branch ID for branch-aware operations
        file_name (str | Unset): File name to operate on
        mode (ModelsYamlGetMode | Unset): IDE mode for YAML operations Default: 'combined'.
        fully_resolved (bool | str | Unset): Resolve the model extends chain so the returned YAML
            reflects what runs at query time. Only valid with mode=combined. Default: 'False'.
        include_checksums (bool | None | Unset): Include checksums in response Default: False.
        include_schemas (str | Unset): A single schema name (optionally catalog-scoped, e.g.
            'warehouse.reporting') to additionally load into the response. Use this to include view
            YAML from a schema that isn't active in the model (inactive or offloaded). Only views from
            this schema will be returned (views with no schema are always included).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelYamlResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            branch_id=branch_id,
            file_name=file_name,
            mode=mode,
            fully_resolved=fully_resolved,
            include_checksums=include_checksums,
            include_schemas=include_schemas,
        )
    ).parsed

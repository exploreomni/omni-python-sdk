from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.content_filter_mode import ContentFilterMode
from ...models.models_content_validator_get_find_type import (
    ModelsContentValidatorGetFindType,
)
from ...models.models_content_validator_get_response import ModelsContentValidatorGetResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    branch_id: UUID | Unset = UNSET,
    content_filter_mode: ContentFilterMode | Unset = UNSET,
    creator_id: UUID | Unset = UNSET,
    find: str | Unset = UNSET,
    find_type: ModelsContentValidatorGetFindType | Unset = UNSET,
    folder_paths: list[str] | Unset = UNSET,
    include_personal_folders: bool | Unset = UNSET,
    labels: str | Unset = UNSET,
    user_id: str | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_branch_id: str | Unset = UNSET
    if not isinstance(branch_id, Unset):
        json_branch_id = str(branch_id)
    params["branch_id"] = json_branch_id

    json_content_filter_mode: str | Unset = UNSET
    if not isinstance(content_filter_mode, Unset):
        json_content_filter_mode = content_filter_mode

    params["content_filter_mode"] = json_content_filter_mode

    json_creator_id: str | Unset = UNSET
    if not isinstance(creator_id, Unset):
        json_creator_id = str(creator_id)
    params["creator_id"] = json_creator_id

    params["find"] = find

    json_find_type: str | Unset = UNSET
    if not isinstance(find_type, Unset):
        json_find_type = find_type

    params["find_type"] = json_find_type

    json_folder_paths: list[str] | Unset = UNSET
    if not isinstance(folder_paths, Unset):
        json_folder_paths = folder_paths

    params["folder_paths"] = json_folder_paths

    params["include_personal_folders"] = include_personal_folders

    params["labels"] = labels

    params["userId"] = user_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/models/{model_id}/content-validator".format(
            model_id=quote(str(model_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsContentValidatorGetResponse | None:
    if response.status_code == 200:
        response_200 = ModelsContentValidatorGetResponse.from_dict(response.json())

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
) -> Response[Any | ModelsContentValidatorGetResponse]:
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
    content_filter_mode: ContentFilterMode | Unset = UNSET,
    creator_id: UUID | Unset = UNSET,
    find: str | Unset = UNSET,
    find_type: ModelsContentValidatorGetFindType | Unset = UNSET,
    folder_paths: list[str] | Unset = UNSET,
    include_personal_folders: bool | Unset = UNSET,
    labels: str | Unset = UNSET,
    user_id: str | Unset = UNSET,
) -> Response[Any | ModelsContentValidatorGetResponse]:
    """Validate content references

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Optional branch ID to validate against. Non-UUID values return
            400.
        content_filter_mode (ContentFilterMode | Unset): Filter documents by issue status. ALL
            (default) returns all documents with at least one query. WITH_ISSUES returns only
            documents with at least one query issue, dashboard filter issue, or document error.
            NO_ISSUES returns only documents with zero issues and no document errors.
        creator_id (UUID | Unset): Filter to documents created by this user (user ID). Unknown IDs
            return 400.
        find (str | Unset): Optional value to find. Used with find_type to scope validation to a
            single view, field, or topic. Requires find_type to be provided.
        find_type (ModelsContentValidatorGetFindType | Unset): Optional type of find operation
            (VIEW, FIELD, TOPIC). Requires find to be provided. FIELD values must be scoped by view
            name (e.g. view_name.field_name).
        folder_paths (list[str] | Unset): Prefix-match folder paths. "/Finance" matches
            "/Finance/Reports". Documents with no folder are excluded unless "" is specified.
        include_personal_folders (bool | Unset): Whether to include personal folders in validation
        labels (str | Unset): Comma-separated label names. Documents matching any label are
            included. Unknown labels return 400.
        user_id (str | Unset): Optional user ID for scoping

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsContentValidatorGetResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        content_filter_mode=content_filter_mode,
        creator_id=creator_id,
        find=find,
        find_type=find_type,
        folder_paths=folder_paths,
        include_personal_folders=include_personal_folders,
        labels=labels,
        user_id=user_id,
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
    content_filter_mode: ContentFilterMode | Unset = UNSET,
    creator_id: UUID | Unset = UNSET,
    find: str | Unset = UNSET,
    find_type: ModelsContentValidatorGetFindType | Unset = UNSET,
    folder_paths: list[str] | Unset = UNSET,
    include_personal_folders: bool | Unset = UNSET,
    labels: str | Unset = UNSET,
    user_id: str | Unset = UNSET,
) -> Any | ModelsContentValidatorGetResponse | None:
    """Validate content references

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Optional branch ID to validate against. Non-UUID values return
            400.
        content_filter_mode (ContentFilterMode | Unset): Filter documents by issue status. ALL
            (default) returns all documents with at least one query. WITH_ISSUES returns only
            documents with at least one query issue, dashboard filter issue, or document error.
            NO_ISSUES returns only documents with zero issues and no document errors.
        creator_id (UUID | Unset): Filter to documents created by this user (user ID). Unknown IDs
            return 400.
        find (str | Unset): Optional value to find. Used with find_type to scope validation to a
            single view, field, or topic. Requires find_type to be provided.
        find_type (ModelsContentValidatorGetFindType | Unset): Optional type of find operation
            (VIEW, FIELD, TOPIC). Requires find to be provided. FIELD values must be scoped by view
            name (e.g. view_name.field_name).
        folder_paths (list[str] | Unset): Prefix-match folder paths. "/Finance" matches
            "/Finance/Reports". Documents with no folder are excluded unless "" is specified.
        include_personal_folders (bool | Unset): Whether to include personal folders in validation
        labels (str | Unset): Comma-separated label names. Documents matching any label are
            included. Unknown labels return 400.
        user_id (str | Unset): Optional user ID for scoping

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsContentValidatorGetResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        branch_id=branch_id,
        content_filter_mode=content_filter_mode,
        creator_id=creator_id,
        find=find,
        find_type=find_type,
        folder_paths=folder_paths,
        include_personal_folders=include_personal_folders,
        labels=labels,
        user_id=user_id,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    content_filter_mode: ContentFilterMode | Unset = UNSET,
    creator_id: UUID | Unset = UNSET,
    find: str | Unset = UNSET,
    find_type: ModelsContentValidatorGetFindType | Unset = UNSET,
    folder_paths: list[str] | Unset = UNSET,
    include_personal_folders: bool | Unset = UNSET,
    labels: str | Unset = UNSET,
    user_id: str | Unset = UNSET,
) -> Response[Any | ModelsContentValidatorGetResponse]:
    """Validate content references

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Optional branch ID to validate against. Non-UUID values return
            400.
        content_filter_mode (ContentFilterMode | Unset): Filter documents by issue status. ALL
            (default) returns all documents with at least one query. WITH_ISSUES returns only
            documents with at least one query issue, dashboard filter issue, or document error.
            NO_ISSUES returns only documents with zero issues and no document errors.
        creator_id (UUID | Unset): Filter to documents created by this user (user ID). Unknown IDs
            return 400.
        find (str | Unset): Optional value to find. Used with find_type to scope validation to a
            single view, field, or topic. Requires find_type to be provided.
        find_type (ModelsContentValidatorGetFindType | Unset): Optional type of find operation
            (VIEW, FIELD, TOPIC). Requires find to be provided. FIELD values must be scoped by view
            name (e.g. view_name.field_name).
        folder_paths (list[str] | Unset): Prefix-match folder paths. "/Finance" matches
            "/Finance/Reports". Documents with no folder are excluded unless "" is specified.
        include_personal_folders (bool | Unset): Whether to include personal folders in validation
        labels (str | Unset): Comma-separated label names. Documents matching any label are
            included. Unknown labels return 400.
        user_id (str | Unset): Optional user ID for scoping

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsContentValidatorGetResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        branch_id=branch_id,
        content_filter_mode=content_filter_mode,
        creator_id=creator_id,
        find=find,
        find_type=find_type,
        folder_paths=folder_paths,
        include_personal_folders=include_personal_folders,
        labels=labels,
        user_id=user_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    branch_id: UUID | Unset = UNSET,
    content_filter_mode: ContentFilterMode | Unset = UNSET,
    creator_id: UUID | Unset = UNSET,
    find: str | Unset = UNSET,
    find_type: ModelsContentValidatorGetFindType | Unset = UNSET,
    folder_paths: list[str] | Unset = UNSET,
    include_personal_folders: bool | Unset = UNSET,
    labels: str | Unset = UNSET,
    user_id: str | Unset = UNSET,
) -> Any | ModelsContentValidatorGetResponse | None:
    """Validate content references

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        branch_id (UUID | Unset): Optional branch ID to validate against. Non-UUID values return
            400.
        content_filter_mode (ContentFilterMode | Unset): Filter documents by issue status. ALL
            (default) returns all documents with at least one query. WITH_ISSUES returns only
            documents with at least one query issue, dashboard filter issue, or document error.
            NO_ISSUES returns only documents with zero issues and no document errors.
        creator_id (UUID | Unset): Filter to documents created by this user (user ID). Unknown IDs
            return 400.
        find (str | Unset): Optional value to find. Used with find_type to scope validation to a
            single view, field, or topic. Requires find_type to be provided.
        find_type (ModelsContentValidatorGetFindType | Unset): Optional type of find operation
            (VIEW, FIELD, TOPIC). Requires find to be provided. FIELD values must be scoped by view
            name (e.g. view_name.field_name).
        folder_paths (list[str] | Unset): Prefix-match folder paths. "/Finance" matches
            "/Finance/Reports". Documents with no folder are excluded unless "" is specified.
        include_personal_folders (bool | Unset): Whether to include personal folders in validation
        labels (str | Unset): Comma-separated label names. Documents matching any label are
            included. Unknown labels return 400.
        user_id (str | Unset): Optional user ID for scoping

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsContentValidatorGetResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            branch_id=branch_id,
            content_filter_mode=content_filter_mode,
            creator_id=creator_id,
            find=find,
            find_type=find_type,
            folder_paths=folder_paths,
            include_personal_folders=include_personal_folders,
            labels=labels,
            user_id=user_id,
        )
    ).parsed

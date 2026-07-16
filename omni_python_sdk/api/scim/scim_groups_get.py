from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.scim_group_response import ScimGroupResponse
from ...models.scim_groups_get_excluded_attributes import (
    ScimGroupsGetExcludedAttributes,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    mini_uuid: str,
    *,
    excluded_attributes: ScimGroupsGetExcludedAttributes | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_excluded_attributes: str | Unset = UNSET
    if not isinstance(excluded_attributes, Unset):
        json_excluded_attributes = excluded_attributes

    params["excludedAttributes"] = json_excluded_attributes

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/scim/v2/Groups/{mini_uuid}".format(
            mini_uuid=quote(str(mini_uuid), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ScimGroupResponse | None:
    if response.status_code == 200:
        response_200 = ScimGroupResponse.from_dict(response.json())

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
) -> Response[Any | ScimGroupResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    excluded_attributes: ScimGroupsGetExcludedAttributes | Unset = UNSET,
) -> Response[Any | ScimGroupResponse]:
    """Get SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        excluded_attributes (ScimGroupsGetExcludedAttributes | Unset): Attributes to exclude from
            the response Example: members.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimGroupResponse]
    """

    kwargs = _get_kwargs(
        mini_uuid=mini_uuid,
        excluded_attributes=excluded_attributes,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    excluded_attributes: ScimGroupsGetExcludedAttributes | Unset = UNSET,
) -> Any | ScimGroupResponse | None:
    """Get SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        excluded_attributes (ScimGroupsGetExcludedAttributes | Unset): Attributes to exclude from
            the response Example: members.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScimGroupResponse
    """

    return sync_detailed(
        mini_uuid=mini_uuid,
        client=client,
        excluded_attributes=excluded_attributes,
    ).parsed


async def asyncio_detailed(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    excluded_attributes: ScimGroupsGetExcludedAttributes | Unset = UNSET,
) -> Response[Any | ScimGroupResponse]:
    """Get SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        excluded_attributes (ScimGroupsGetExcludedAttributes | Unset): Attributes to exclude from
            the response Example: members.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimGroupResponse]
    """

    kwargs = _get_kwargs(
        mini_uuid=mini_uuid,
        excluded_attributes=excluded_attributes,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    excluded_attributes: ScimGroupsGetExcludedAttributes | Unset = UNSET,
) -> Any | ScimGroupResponse | None:
    """Get SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        excluded_attributes (ScimGroupsGetExcludedAttributes | Unset): Attributes to exclude from
            the response Example: members.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScimGroupResponse
    """

    return (
        await asyncio_detailed(
            mini_uuid=mini_uuid,
            client=client,
            excluded_attributes=excluded_attributes,
        )
    ).parsed

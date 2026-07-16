from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.scim_group_response import ScimGroupResponse
from ...models.scim_groups_replace_body import ScimGroupsReplaceBody
from ...types import UNSET, Response, Unset


def _get_kwargs(
    mini_uuid: str,
    *,
    body: ScimGroupsReplaceBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/scim/v2/Groups/{mini_uuid}".format(
            mini_uuid=quote(str(mini_uuid), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ScimGroupResponse | None:
    if response.status_code == 200:
        response_200 = ScimGroupResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

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
    body: ScimGroupsReplaceBody | Unset = UNSET,
) -> Response[Any | ScimGroupResponse]:
    """Replace SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        body (ScimGroupsReplaceBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimGroupResponse]
    """

    kwargs = _get_kwargs(
        mini_uuid=mini_uuid,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    body: ScimGroupsReplaceBody | Unset = UNSET,
) -> Any | ScimGroupResponse | None:
    """Replace SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        body (ScimGroupsReplaceBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ScimGroupResponse
    """

    return sync_detailed(
        mini_uuid=mini_uuid,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    body: ScimGroupsReplaceBody | Unset = UNSET,
) -> Response[Any | ScimGroupResponse]:
    """Replace SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        body (ScimGroupsReplaceBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ScimGroupResponse]
    """

    kwargs = _get_kwargs(
        mini_uuid=mini_uuid,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    mini_uuid: str,
    *,
    client: AuthenticatedClient | Client,
    body: ScimGroupsReplaceBody | Unset = UNSET,
) -> Any | ScimGroupResponse | None:
    """Replace SCIM group

    Args:
        mini_uuid (str): Short identifier of the group Example: abc123.
        body (ScimGroupsReplaceBody | Unset):

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
            body=body,
        )
    ).parsed

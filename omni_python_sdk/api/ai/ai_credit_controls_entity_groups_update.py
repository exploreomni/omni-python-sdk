from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_entity_group_credit_limits_response import AiEntityGroupCreditLimitsResponse
from ...models.ai_entity_group_credit_limits_update_body import AiEntityGroupCreditLimitsUpdateBody
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...models.api_error_404 import ApiError404
from ...types import Response


def _get_kwargs(
    *,
    body: AiEntityGroupCreditLimitsUpdateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/ai/credit-controls/entity-groups",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    if response.status_code == 200:
        response_200 = AiEntityGroupCreditLimitsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ApiError403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ApiError404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiEntityGroupCreditLimitsUpdateBody,
) -> Response[AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Set individual entity groups' AI credit limits

     Set individual embed entity groups' AI credit limits in bulk. Each entry names an entity group by
    its embed `entity` string and either sets an individual limit (`creditLimit`: a non-negative number,
    or `null` for unlimited) or removes one (`useDefaultLimit: true`) so the entity group follows the
    org default. Each entity may appear at most once and must have an entity group in the organization.
    All updates are applied in one transaction, so either every entry takes effect or none do — an
    unknown entity fails the whole request with a 404 naming it. Requires the same add/remove-users
    permission as the group settings page and the embed-entity credit-limit feature flag.

    Args:
        body (AiEntityGroupCreditLimitsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]
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
    body: AiEntityGroupCreditLimitsUpdateBody,
) -> AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Set individual entity groups' AI credit limits

     Set individual embed entity groups' AI credit limits in bulk. Each entry names an entity group by
    its embed `entity` string and either sets an individual limit (`creditLimit`: a non-negative number,
    or `null` for unlimited) or removes one (`useDefaultLimit: true`) so the entity group follows the
    org default. Each entity may appear at most once and must have an entity group in the organization.
    All updates are applied in one transaction, so either every entry takes effect or none do — an
    unknown entity fails the whole request with a 404 naming it. Requires the same add/remove-users
    permission as the group settings page and the embed-entity credit-limit feature flag.

    Args:
        body (AiEntityGroupCreditLimitsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiEntityGroupCreditLimitsUpdateBody,
) -> Response[AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]:
    """Set individual entity groups' AI credit limits

     Set individual embed entity groups' AI credit limits in bulk. Each entry names an entity group by
    its embed `entity` string and either sets an individual limit (`creditLimit`: a non-negative number,
    or `null` for unlimited) or removes one (`useDefaultLimit: true`) so the entity group follows the
    org default. Each entity may appear at most once and must have an entity group in the organization.
    All updates are applied in one transaction, so either every entry takes effect or none do — an
    unknown entity fails the whole request with a 404 naming it. Requires the same add/remove-users
    permission as the group settings page and the embed-entity credit-limit feature flag.

    Args:
        body (AiEntityGroupCreditLimitsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiEntityGroupCreditLimitsUpdateBody,
) -> AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404 | None:
    """Set individual entity groups' AI credit limits

     Set individual embed entity groups' AI credit limits in bulk. Each entry names an entity group by
    its embed `entity` string and either sets an individual limit (`creditLimit`: a non-negative number,
    or `null` for unlimited) or removes one (`useDefaultLimit: true`) so the entity group follows the
    org default. Each entity may appear at most once and must have an entity group in the organization.
    All updates are applied in one transaction, so either every entry takes effect or none do — an
    unknown entity fails the whole request with a 404 naming it. Requires the same add/remove-users
    permission as the group settings page and the embed-entity credit-limit feature flag.

    Args:
        body (AiEntityGroupCreditLimitsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiEntityGroupCreditLimitsResponse | ApiError400 | ApiError401 | ApiError403 | ApiError404
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed

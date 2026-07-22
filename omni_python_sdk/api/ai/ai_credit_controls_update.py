from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ai_credit_controls_response import AiCreditControlsResponse
from ...models.ai_credit_controls_update_body import AiCreditControlsUpdateBody
from ...models.api_error_400 import ApiError400
from ...models.api_error_401 import ApiError401
from ...models.api_error_403 import ApiError403
from ...types import Response


def _get_kwargs(
    *,
    body: AiCreditControlsUpdateBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/ai/credit-controls",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403 | None:
    if response.status_code == 200:
        response_200 = AiCreditControlsResponse.from_dict(response.json())

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiCreditControlsUpdateBody,
) -> Response[AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403]:
    """Update AI credit controls

     Update the organization's AI credit controls: the downgrade and shutoff thresholds, the default per-
    user credit limit (userDefaultCredits), and the default per-entity-group credit limit
    (entityGroupDefaultCredits). All fields are optional and tri-state: omit a field to leave it
    unchanged, send `null` to turn that control off (for the defaults: unlimited by default), or send a
    non-negative number to set it. At least one field is required. The `downgradeCredits <=
    shutoffCredits` invariant is enforced against the merged result. Returns the full current state, the
    same shape as GET.

    Args:
        body (AiCreditControlsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403]
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
    body: AiCreditControlsUpdateBody,
) -> AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403 | None:
    """Update AI credit controls

     Update the organization's AI credit controls: the downgrade and shutoff thresholds, the default per-
    user credit limit (userDefaultCredits), and the default per-entity-group credit limit
    (entityGroupDefaultCredits). All fields are optional and tri-state: omit a field to leave it
    unchanged, send `null` to turn that control off (for the defaults: unlimited by default), or send a
    non-negative number to set it. At least one field is required. The `downgradeCredits <=
    shutoffCredits` invariant is enforced against the merged result. Returns the full current state, the
    same shape as GET.

    Args:
        body (AiCreditControlsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: AiCreditControlsUpdateBody,
) -> Response[AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403]:
    """Update AI credit controls

     Update the organization's AI credit controls: the downgrade and shutoff thresholds, the default per-
    user credit limit (userDefaultCredits), and the default per-entity-group credit limit
    (entityGroupDefaultCredits). All fields are optional and tri-state: omit a field to leave it
    unchanged, send `null` to turn that control off (for the defaults: unlimited by default), or send a
    non-negative number to set it. At least one field is required. The `downgradeCredits <=
    shutoffCredits` invariant is enforced against the merged result. Returns the full current state, the
    same shape as GET.

    Args:
        body (AiCreditControlsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: AiCreditControlsUpdateBody,
) -> AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403 | None:
    """Update AI credit controls

     Update the organization's AI credit controls: the downgrade and shutoff thresholds, the default per-
    user credit limit (userDefaultCredits), and the default per-entity-group credit limit
    (entityGroupDefaultCredits). All fields are optional and tri-state: omit a field to leave it
    unchanged, send `null` to turn that control off (for the defaults: unlimited by default), or send a
    non-negative number to set it. At least one field is required. The `downgradeCredits <=
    shutoffCredits` invariant is enforced against the merged result. Returns the full current state, the
    same shape as GET.

    Args:
        body (AiCreditControlsUpdateBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AiCreditControlsResponse | ApiError400 | ApiError401 | ApiError403
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed

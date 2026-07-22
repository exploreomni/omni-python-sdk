from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.models_git_update_body import ModelsGitUpdateBody
from ...models.models_git_update_response import ModelsGitUpdateResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    model_id: UUID,
    *,
    body: ModelsGitUpdateBody | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/models/{model_id}/git".format(
            model_id=quote(str(model_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | ModelsGitUpdateResponse | None:
    if response.status_code == 200:
        response_200 = ModelsGitUpdateResponse.from_dict(response.json())

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
) -> Response[Any | ModelsGitUpdateResponse]:
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
    body: ModelsGitUpdateBody | Unset = UNSET,
) -> Response[Any | ModelsGitUpdateResponse]:
    """Update git configuration

     Update git configuration for a model. Only provided fields are changed. For SSH auth, a bring-your-
    own deploy key can be set via deployPrivateKey (with deployKeyPassphrase for encrypted keys),
    enabling zero-downtime key rotation: authorize the matching public key with the git provider first,
    then set the key here.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsGitUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsGitUpdateResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsGitUpdateBody | Unset = UNSET,
) -> Any | ModelsGitUpdateResponse | None:
    """Update git configuration

     Update git configuration for a model. Only provided fields are changed. For SSH auth, a bring-your-
    own deploy key can be set via deployPrivateKey (with deployKeyPassphrase for encrypted keys),
    enabling zero-downtime key rotation: authorize the matching public key with the git provider first,
    then set the key here.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsGitUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsGitUpdateResponse
    """

    return sync_detailed(
        model_id=model_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsGitUpdateBody | Unset = UNSET,
) -> Response[Any | ModelsGitUpdateResponse]:
    """Update git configuration

     Update git configuration for a model. Only provided fields are changed. For SSH auth, a bring-your-
    own deploy key can be set via deployPrivateKey (with deployKeyPassphrase for encrypted keys),
    enabling zero-downtime key rotation: authorize the matching public key with the git provider first,
    then set the key here.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsGitUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ModelsGitUpdateResponse]
    """

    kwargs = _get_kwargs(
        model_id=model_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    model_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: ModelsGitUpdateBody | Unset = UNSET,
) -> Any | ModelsGitUpdateResponse | None:
    """Update git configuration

     Update git configuration for a model. Only provided fields are changed. For SSH auth, a bring-your-
    own deploy key can be set via deployPrivateKey (with deployKeyPassphrase for encrypted keys),
    enabling zero-downtime key rotation: authorize the matching public key with the git provider first,
    then set the key here.

    Args:
        model_id (UUID): Model UUID Example: 123e4567-e89b-12d3-a456-426614174000.
        body (ModelsGitUpdateBody | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ModelsGitUpdateResponse
    """

    return (
        await asyncio_detailed(
            model_id=model_id,
            client=client,
            body=body,
        )
    ).parsed

"""Small SCIM conveniences shared by the user management examples.

These reimplement the upsert/group-membership helpers from the 0.x SDK on
top of the generated client.
"""

from uuid import UUID

from omni_python_sdk import AuthenticatedClient
from omni_python_sdk.api.scim import (
    scim_groups_get,
    scim_groups_list,
    scim_groups_replace,
    scim_users_create,
    scim_users_delete,
    scim_users_list,
    scim_users_replace,
)
from omni_python_sdk.models import ScimGroupsReplaceBody, ScimUserCreateRequest, ScimUserPutRequest


def listify(d: dict) -> dict:
    """Convert string representations of lists ("[a,b]") to actual lists."""
    out = {}
    for k, v in d.items():
        if '[' in v and ']' in v:
            out.update({k: [item for item in v.replace('[', '').replace(']', '').split(',')]})
        else:
            out.update({k: v})
    return out


def return_user_by_email(client: AuthenticatedClient, email: str) -> dict | None:
    """Find a user by email; returns the SCIM user dict, or None if not exactly one match."""
    response = scim_users_list.sync(client=client, filter_=f'userName eq "{email}"')
    users = response.to_dict()['Resources']
    if len(users) == 1:
        return users[0]
    print(f"Found {len(users)} users for {email}")
    return None


def upsert_user(client: AuthenticatedClient, email: str, display_name: str, attributes: dict) -> None:
    """Create the user, or replace it if it already exists."""
    body = {
        "urn:omni:params:1.0:UserAttribute": listify(attributes),
        "userName": email,
        "displayName": display_name,
    }
    response = scim_users_list.sync(client=client, filter_=f'userName eq "{email}"')
    users = response.to_dict()['Resources']
    if len(users) == 1:
        user = users[0]
        scim_users_replace.sync(UUID(user['id']), client=client, body=ScimUserPutRequest.from_dict(body))
        print(f"updated user id {user['id']}")
    elif len(users) == 0:
        created = scim_users_create.sync(client=client, body=ScimUserCreateRequest.from_dict(body))
        print(f"Created {email}, userid: {created.to_dict()['id']}")
    else:
        print(f'{len(users)} found for {email}, no action taken')


def delete_user(client: AuthenticatedClient, email: str) -> None:
    """Delete a user by email address."""
    user = return_user_by_email(client, email)
    if user is None:
        print(f'user {email} not found')
        return
    response = scim_users_delete.sync_detailed(UUID(user['id']), client=client)
    if response.status_code == 204:
        print(f"deleted userid: {user['id']} email: {email}")
    else:
        print(f"Error ({response.status_code}) deleting user id {user['id']}")


def get_group_id(client: AuthenticatedClient, group_name: str) -> str | None:
    """Get the ID of a group by display name (paginates through all groups)."""
    count = 100
    start_index = 1
    while True:
        response = scim_groups_list.sync(client=client, count=str(count), start_index=str(start_index)).to_dict()
        group = next((g for g in response['Resources'] if g['displayName'] == group_name), None)
        if group:
            return group['id']
        if response['totalResults'] <= start_index:
            return None
        start_index += count


def _update_group_members(client: AuthenticatedClient, group_name: str, mutate) -> None:
    group_id = get_group_id(client, group_name)
    if not group_id:
        raise ValueError(f"Group '{group_name}' not found.")
    group = scim_groups_get.sync(group_id, client=client).to_dict()
    group['members'] = mutate(group.get('members', []))
    scim_groups_replace.sync(group_id, client=client, body=ScimGroupsReplaceBody.from_dict(group))


def add_user_to_group(client: AuthenticatedClient, group_name: str, user_id: str) -> None:
    """Add a user to a group by group display name."""
    _update_group_members(client, group_name, lambda members: members + [{"display": '', "value": user_id}])


def remove_user_from_group(client: AuthenticatedClient, group_name: str, user_id: str) -> None:
    """Remove a user from a group by group display name."""
    _update_group_members(
        client, group_name, lambda members: [m for m in members if m['value'] != user_id]
    )

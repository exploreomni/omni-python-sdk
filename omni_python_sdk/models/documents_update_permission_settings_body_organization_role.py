from typing import Literal

DocumentsUpdatePermissionSettingsBodyOrganizationRole = Literal["editor", "manager", "no_access", "viewer"]

DOCUMENTS_UPDATE_PERMISSION_SETTINGS_BODY_ORGANIZATION_ROLE_VALUES: set[
    DocumentsUpdatePermissionSettingsBodyOrganizationRole
] = {
    "editor",
    "manager",
    "no_access",
    "viewer",
}


def check_documents_update_permission_settings_body_organization_role(
    value: str,
) -> DocumentsUpdatePermissionSettingsBodyOrganizationRole:
    if value in DOCUMENTS_UPDATE_PERMISSION_SETTINGS_BODY_ORGANIZATION_ROLE_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {DOCUMENTS_UPDATE_PERMISSION_SETTINGS_BODY_ORGANIZATION_ROLE_VALUES!r}"
    )

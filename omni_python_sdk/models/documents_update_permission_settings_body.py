from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.documents_update_permission_settings_body_organization_role import (
    DocumentsUpdatePermissionSettingsBodyOrganizationRole,
    check_documents_update_permission_settings_body_organization_role,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentsUpdatePermissionSettingsBody")


@_attrs_define
class DocumentsUpdatePermissionSettingsBody:
    """
    Attributes:
        can_analyze (bool | Unset): Allow exploring from this document
        can_download (bool | Unset): Allow downloading
        can_drill (bool | Unset): Allow drill-down
        can_duplicate (bool | Unset): Allow duplicating
        can_request_access (bool | Unset): Allow requesting access
        can_save_spreadsheets (bool | Unset): Allow creating spreadsheets
        can_schedule (bool | Unset): Allow scheduling
        can_upload (bool | Unset): Allow uploads
        can_use_dashboard_ai (bool | Unset): Allow using dashboard AI
        can_use_timezone_override (bool | Unset): Allow timezone override
        can_view_workbook (bool | Unset): Allow viewing workbook
        organization_access_boost (bool | Unset): Boost organization access
        organization_role (DocumentsUpdatePermissionSettingsBodyOrganizationRole | Unset): Organization-wide role for
            the document
        require_pull_request_to_publish (bool | Unset): Require pull request to publish changes
    """

    can_analyze: bool | Unset = UNSET
    can_download: bool | Unset = UNSET
    can_drill: bool | Unset = UNSET
    can_duplicate: bool | Unset = UNSET
    can_request_access: bool | Unset = UNSET
    can_save_spreadsheets: bool | Unset = UNSET
    can_schedule: bool | Unset = UNSET
    can_upload: bool | Unset = UNSET
    can_use_dashboard_ai: bool | Unset = UNSET
    can_use_timezone_override: bool | Unset = UNSET
    can_view_workbook: bool | Unset = UNSET
    organization_access_boost: bool | Unset = UNSET
    organization_role: DocumentsUpdatePermissionSettingsBodyOrganizationRole | Unset = UNSET
    require_pull_request_to_publish: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        can_analyze = self.can_analyze

        can_download = self.can_download

        can_drill = self.can_drill

        can_duplicate = self.can_duplicate

        can_request_access = self.can_request_access

        can_save_spreadsheets = self.can_save_spreadsheets

        can_schedule = self.can_schedule

        can_upload = self.can_upload

        can_use_dashboard_ai = self.can_use_dashboard_ai

        can_use_timezone_override = self.can_use_timezone_override

        can_view_workbook = self.can_view_workbook

        organization_access_boost = self.organization_access_boost

        organization_role: str | Unset = UNSET
        if not isinstance(self.organization_role, Unset):
            organization_role = self.organization_role

        require_pull_request_to_publish = self.require_pull_request_to_publish

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if can_analyze is not UNSET:
            field_dict["canAnalyze"] = can_analyze
        if can_download is not UNSET:
            field_dict["canDownload"] = can_download
        if can_drill is not UNSET:
            field_dict["canDrill"] = can_drill
        if can_duplicate is not UNSET:
            field_dict["canDuplicate"] = can_duplicate
        if can_request_access is not UNSET:
            field_dict["canRequestAccess"] = can_request_access
        if can_save_spreadsheets is not UNSET:
            field_dict["canSaveSpreadsheets"] = can_save_spreadsheets
        if can_schedule is not UNSET:
            field_dict["canSchedule"] = can_schedule
        if can_upload is not UNSET:
            field_dict["canUpload"] = can_upload
        if can_use_dashboard_ai is not UNSET:
            field_dict["canUseDashboardAi"] = can_use_dashboard_ai
        if can_use_timezone_override is not UNSET:
            field_dict["canUseTimezoneOverride"] = can_use_timezone_override
        if can_view_workbook is not UNSET:
            field_dict["canViewWorkbook"] = can_view_workbook
        if organization_access_boost is not UNSET:
            field_dict["organizationAccessBoost"] = organization_access_boost
        if organization_role is not UNSET:
            field_dict["organizationRole"] = organization_role
        if require_pull_request_to_publish is not UNSET:
            field_dict["requirePullRequestToPublish"] = require_pull_request_to_publish

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        can_analyze = d.pop("canAnalyze", UNSET)

        can_download = d.pop("canDownload", UNSET)

        can_drill = d.pop("canDrill", UNSET)

        can_duplicate = d.pop("canDuplicate", UNSET)

        can_request_access = d.pop("canRequestAccess", UNSET)

        can_save_spreadsheets = d.pop("canSaveSpreadsheets", UNSET)

        can_schedule = d.pop("canSchedule", UNSET)

        can_upload = d.pop("canUpload", UNSET)

        can_use_dashboard_ai = d.pop("canUseDashboardAi", UNSET)

        can_use_timezone_override = d.pop("canUseTimezoneOverride", UNSET)

        can_view_workbook = d.pop("canViewWorkbook", UNSET)

        organization_access_boost = d.pop("organizationAccessBoost", UNSET)

        _organization_role = d.pop("organizationRole", UNSET)
        organization_role: DocumentsUpdatePermissionSettingsBodyOrganizationRole | Unset
        if isinstance(_organization_role, Unset):
            organization_role = UNSET
        else:
            organization_role = check_documents_update_permission_settings_body_organization_role(_organization_role)

        require_pull_request_to_publish = d.pop("requirePullRequestToPublish", UNSET)

        documents_update_permission_settings_body = cls(
            can_analyze=can_analyze,
            can_download=can_download,
            can_drill=can_drill,
            can_duplicate=can_duplicate,
            can_request_access=can_request_access,
            can_save_spreadsheets=can_save_spreadsheets,
            can_schedule=can_schedule,
            can_upload=can_upload,
            can_use_dashboard_ai=can_use_dashboard_ai,
            can_use_timezone_override=can_use_timezone_override,
            can_view_workbook=can_view_workbook,
            organization_access_boost=organization_access_boost,
            organization_role=organization_role,
            require_pull_request_to_publish=require_pull_request_to_publish,
        )

        documents_update_permission_settings_body.additional_properties = d
        return documents_update_permission_settings_body

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties

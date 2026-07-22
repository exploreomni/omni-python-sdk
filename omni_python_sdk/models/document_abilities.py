from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentAbilities")


@_attrs_define
class DocumentAbilities:
    """Document-level ability values, as stored on the document

    Attributes:
        can_analyze (bool): Allow exploring from this document
        can_download (bool): Allow downloading
        can_drill (bool): Allow drill-down
        can_duplicate (bool): Allow duplicating
        can_request_access (bool): Allow requesting access
        can_save_spreadsheets (bool): Allow creating spreadsheets
        can_schedule (bool): Allow scheduling
        can_upload (bool): Allow uploads
        can_use_dashboard_ai (bool): Allow using dashboard AI
        can_use_timezone_override (bool): Allow timezone override
        can_view_workbook (bool): Allow viewing workbook
        require_pull_request_to_publish (bool): Require pull request to publish changes
    """

    can_analyze: bool
    can_download: bool
    can_drill: bool
    can_duplicate: bool
    can_request_access: bool
    can_save_spreadsheets: bool
    can_schedule: bool
    can_upload: bool
    can_use_dashboard_ai: bool
    can_use_timezone_override: bool
    can_view_workbook: bool
    require_pull_request_to_publish: bool
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

        require_pull_request_to_publish = self.require_pull_request_to_publish

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "canAnalyze": can_analyze,
                "canDownload": can_download,
                "canDrill": can_drill,
                "canDuplicate": can_duplicate,
                "canRequestAccess": can_request_access,
                "canSaveSpreadsheets": can_save_spreadsheets,
                "canSchedule": can_schedule,
                "canUpload": can_upload,
                "canUseDashboardAi": can_use_dashboard_ai,
                "canUseTimezoneOverride": can_use_timezone_override,
                "canViewWorkbook": can_view_workbook,
                "requirePullRequestToPublish": require_pull_request_to_publish,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        can_analyze = d.pop("canAnalyze")

        can_download = d.pop("canDownload")

        can_drill = d.pop("canDrill")

        can_duplicate = d.pop("canDuplicate")

        can_request_access = d.pop("canRequestAccess")

        can_save_spreadsheets = d.pop("canSaveSpreadsheets")

        can_schedule = d.pop("canSchedule")

        can_upload = d.pop("canUpload")

        can_use_dashboard_ai = d.pop("canUseDashboardAi")

        can_use_timezone_override = d.pop("canUseTimezoneOverride")

        can_view_workbook = d.pop("canViewWorkbook")

        require_pull_request_to_publish = d.pop("requirePullRequestToPublish")

        document_abilities = cls(
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
            require_pull_request_to_publish=require_pull_request_to_publish,
        )

        document_abilities.additional_properties = d
        return document_abilities

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

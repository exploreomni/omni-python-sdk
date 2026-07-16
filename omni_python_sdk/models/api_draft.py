from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.api_draft_status import ApiDraftStatus, check_api_draft_status

if TYPE_CHECKING:
    from ..models.api_draft_actor import ApiDraftActor
    from ..models.api_draft_branch_type_0 import ApiDraftBranchType0


T = TypeVar("T", bound="ApiDraft")


@_attrs_define
class ApiDraft:
    """
    Attributes:
        branch (ApiDraftBranchType0 | None): Branch the draft is attached to, or null for a draft on main
        created_at (datetime.datetime): When the draft was created
        created_by (ApiDraftActor): User who created the draft
        draft_out_of_date (bool): True when the published document was published more recently than the draft was
            created (the draft is based on a stale baseline)
        identifier (str): Draft workbook identifier — use this to address the draft
        last_edited_by (ApiDraftActor): User who created the draft
        published_identifier (str): Identifier of the published document the draft is for
        status (ApiDraftStatus): Lifecycle status: "active" for current drafts, "archived" for soft-deleted drafts
            (retained ~7 days)
        updated_at (datetime.datetime): Most recent edit time on the draft workbook
        workbook_model_id (UUID): omni_model ID for the draft workbook
    """

    branch: ApiDraftBranchType0 | None
    created_at: datetime.datetime
    created_by: ApiDraftActor
    draft_out_of_date: bool
    identifier: str
    last_edited_by: ApiDraftActor
    published_identifier: str
    status: ApiDraftStatus
    updated_at: datetime.datetime
    workbook_model_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.api_draft_branch_type_0 import ApiDraftBranchType0

        branch: dict[str, Any] | None
        if isinstance(self.branch, ApiDraftBranchType0):
            branch = self.branch.to_dict()
        else:
            branch = self.branch

        created_at = self.created_at.isoformat()

        created_by = self.created_by.to_dict()

        draft_out_of_date = self.draft_out_of_date

        identifier = self.identifier

        last_edited_by = self.last_edited_by.to_dict()

        published_identifier = self.published_identifier

        status: str = self.status

        updated_at = self.updated_at.isoformat()

        workbook_model_id = str(self.workbook_model_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch": branch,
                "createdAt": created_at,
                "createdBy": created_by,
                "draftOutOfDate": draft_out_of_date,
                "identifier": identifier,
                "lastEditedBy": last_edited_by,
                "publishedIdentifier": published_identifier,
                "status": status,
                "updatedAt": updated_at,
                "workbookModelId": workbook_model_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.api_draft_actor import ApiDraftActor
        from ..models.api_draft_branch_type_0 import ApiDraftBranchType0

        d = dict(src_dict)

        def _parse_branch(data: object) -> ApiDraftBranchType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_api_draft_branch_type_0 = ApiDraftBranchType0.from_dict(data)

                return componentsschemas_api_draft_branch_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ApiDraftBranchType0 | None, data)

        branch = _parse_branch(d.pop("branch"))

        created_at = datetime.datetime.fromisoformat(d.pop("createdAt"))

        created_by = ApiDraftActor.from_dict(d.pop("createdBy"))

        draft_out_of_date = d.pop("draftOutOfDate")

        identifier = d.pop("identifier")

        last_edited_by = ApiDraftActor.from_dict(d.pop("lastEditedBy"))

        published_identifier = d.pop("publishedIdentifier")

        status = check_api_draft_status(d.pop("status"))

        updated_at = datetime.datetime.fromisoformat(d.pop("updatedAt"))

        workbook_model_id = UUID(d.pop("workbookModelId"))

        api_draft = cls(
            branch=branch,
            created_at=created_at,
            created_by=created_by,
            draft_out_of_date=draft_out_of_date,
            identifier=identifier,
            last_edited_by=last_edited_by,
            published_identifier=published_identifier,
            status=status,
            updated_at=updated_at,
            workbook_model_id=workbook_model_id,
        )

        api_draft.additional_properties = d
        return api_draft

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

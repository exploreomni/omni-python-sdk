from __future__ import annotations

from collections.abc import Mapping
from io import BytesIO
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from .. import types
from ..types import UNSET, File, Unset

T = TypeVar("T", bound="UploadCreateBody")


@_attrs_define
class UploadCreateBody:
    """
    Attributes:
        file (File): The CSV file to upload
        model_id (UUID): UUID of the model to create the view in
        branch_id (UUID | Unset): UUID of the branch to create the view in (mutually exclusive with branchName)
        branch_name (str | Unset): Name of the branch to create the view in (mutually exclusive with branchId)
        view_name (str | Unset): Override the view name (defaults to sanitized file name)
    """

    file: File
    model_id: UUID
    branch_id: UUID | Unset = UNSET
    branch_name: str | Unset = UNSET
    view_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        file = self.file.to_tuple()

        model_id = str(self.model_id)

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        branch_name = self.branch_name

        view_name = self.view_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "file": file,
                "modelId": model_id,
            }
        )
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if branch_name is not UNSET:
            field_dict["branchName"] = branch_name
        if view_name is not UNSET:
            field_dict["viewName"] = view_name

        return field_dict

    def to_multipart(self) -> types.RequestFiles:
        files: types.RequestFiles = []

        files.append(("file", self.file.to_tuple()))

        files.append(("modelId", (None, str(self.model_id), "text/plain")))

        if not isinstance(self.branch_id, Unset):
            files.append(("branchId", (None, str(self.branch_id), "text/plain")))

        if not isinstance(self.branch_name, Unset):
            files.append(("branchName", (None, str(self.branch_name).encode(), "text/plain")))

        if not isinstance(self.view_name, Unset):
            files.append(("viewName", (None, str(self.view_name).encode(), "text/plain")))

        for prop_name, prop in self.additional_properties.items():
            files.append((prop_name, (None, str(prop).encode(), "text/plain")))

        return files

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        file = File(payload=BytesIO(d.pop("file")))

        model_id = UUID(d.pop("modelId"))

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        branch_name = d.pop("branchName", UNSET)

        view_name = d.pop("viewName", UNSET)

        upload_create_body = cls(
            file=file,
            model_id=model_id,
            branch_id=branch_id,
            branch_name=branch_name,
            view_name=view_name,
        )

        upload_create_body.additional_properties = d
        return upload_create_body

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

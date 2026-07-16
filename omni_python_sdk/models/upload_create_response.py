from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="UploadCreateResponse")


@_attrs_define
class UploadCreateResponse:
    """
    Attributes:
        file_name (str): Original file name Example: users.csv.
        id (UUID): Unique identifier for the upload
        in_db_as_table_name (str): Database table name in the scratch schema
        model_id (UUID): Model ID the view was created in
        row_count (int): Number of rows in the uploaded file
        truncated (bool): Whether the file was truncated due to row limit
        view_created (bool): Whether a view was created in the model
        view_name (str): Name of the view created
    """

    file_name: str
    id: UUID
    in_db_as_table_name: str
    model_id: UUID
    row_count: int
    truncated: bool
    view_created: bool
    view_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        file_name = self.file_name

        id = str(self.id)

        in_db_as_table_name = self.in_db_as_table_name

        model_id = str(self.model_id)

        row_count = self.row_count

        truncated = self.truncated

        view_created = self.view_created

        view_name = self.view_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "fileName": file_name,
                "id": id,
                "inDbAsTableName": in_db_as_table_name,
                "modelId": model_id,
                "rowCount": row_count,
                "truncated": truncated,
                "viewCreated": view_created,
                "viewName": view_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        file_name = d.pop("fileName")

        id = UUID(d.pop("id"))

        in_db_as_table_name = d.pop("inDbAsTableName")

        model_id = UUID(d.pop("modelId"))

        row_count = d.pop("rowCount")

        truncated = d.pop("truncated")

        view_created = d.pop("viewCreated")

        view_name = d.pop("viewName")

        upload_create_response = cls(
            file_name=file_name,
            id=id,
            in_db_as_table_name=in_db_as_table_name,
            model_id=model_id,
            row_count=row_count,
            truncated=truncated,
            view_created=view_created,
            view_name=view_name,
        )

        upload_create_response.additional_properties = d
        return upload_create_response

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

from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.upload_uploaded_by_user_type_0 import UploadUploadedByUserType0


T = TypeVar("T", bound="Upload")


@_attrs_define
class Upload:
    """
    Attributes:
        connection_id (UUID): Connection ID the upload is associated with
        created_at (datetime.datetime): When the file was uploaded
        file_name (str): Original file name Example: users.csv.
        id (UUID): Unique identifier for the upload
        in_db_as_table_name (None | str): Database table name if uploaded to database scratch schema
        model_id (None | UUID): Model ID the upload is associated with (inferred from connection's shared model if not
            explicitly set)
        size_bytes (float | None): File size in bytes
        updated_at (datetime.datetime): Last update timestamp
        uploaded_by_user (None | UploadUploadedByUserType0): User who uploaded the file
        view_name (str): View name associated with the upload
    """

    connection_id: UUID
    created_at: datetime.datetime
    file_name: str
    id: UUID
    in_db_as_table_name: None | str
    model_id: None | UUID
    size_bytes: float | None
    updated_at: datetime.datetime
    uploaded_by_user: None | UploadUploadedByUserType0
    view_name: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.upload_uploaded_by_user_type_0 import UploadUploadedByUserType0

        connection_id = str(self.connection_id)

        created_at = self.created_at.isoformat()

        file_name = self.file_name

        id = str(self.id)

        in_db_as_table_name: None | str
        in_db_as_table_name = self.in_db_as_table_name

        model_id: None | str
        if isinstance(self.model_id, UUID):
            model_id = str(self.model_id)
        else:
            model_id = self.model_id

        size_bytes: float | None
        size_bytes = self.size_bytes

        updated_at = self.updated_at.isoformat()

        uploaded_by_user: dict[str, Any] | None
        if isinstance(self.uploaded_by_user, UploadUploadedByUserType0):
            uploaded_by_user = self.uploaded_by_user.to_dict()
        else:
            uploaded_by_user = self.uploaded_by_user

        view_name = self.view_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connection_id": connection_id,
                "created_at": created_at,
                "file_name": file_name,
                "id": id,
                "in_db_as_table_name": in_db_as_table_name,
                "model_id": model_id,
                "size_bytes": size_bytes,
                "updated_at": updated_at,
                "uploaded_by_user": uploaded_by_user,
                "view_name": view_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.upload_uploaded_by_user_type_0 import UploadUploadedByUserType0

        d = dict(src_dict)
        connection_id = UUID(d.pop("connection_id"))

        created_at = datetime.datetime.fromisoformat(d.pop("created_at"))

        file_name = d.pop("file_name")

        id = UUID(d.pop("id"))

        def _parse_in_db_as_table_name(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        in_db_as_table_name = _parse_in_db_as_table_name(d.pop("in_db_as_table_name"))

        def _parse_model_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                model_id_type_0 = UUID(data)

                return model_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        model_id = _parse_model_id(d.pop("model_id"))

        def _parse_size_bytes(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        size_bytes = _parse_size_bytes(d.pop("size_bytes"))

        updated_at = datetime.datetime.fromisoformat(d.pop("updated_at"))

        def _parse_uploaded_by_user(data: object) -> None | UploadUploadedByUserType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                uploaded_by_user_type_0 = UploadUploadedByUserType0.from_dict(data)

                return uploaded_by_user_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UploadUploadedByUserType0, data)

        uploaded_by_user = _parse_uploaded_by_user(d.pop("uploaded_by_user"))

        view_name = d.pop("view_name")

        upload = cls(
            connection_id=connection_id,
            created_at=created_at,
            file_name=file_name,
            id=id,
            in_db_as_table_name=in_db_as_table_name,
            model_id=model_id,
            size_bytes=size_bytes,
            updated_at=updated_at,
            uploaded_by_user=uploaded_by_user,
            view_name=view_name,
        )

        upload.additional_properties = d
        return upload

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

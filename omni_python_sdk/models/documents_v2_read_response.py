from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.containers_item import ContainersItem
    from ..models.controls_read_external import ControlsReadExternal
    from ..models.query_presentations_read_external import QueryPresentationsReadExternal
    from ..models.settings_read_external import SettingsReadExternal


T = TypeVar("T", bound="DocumentsV2ReadResponse")


@_attrs_define
class DocumentsV2ReadResponse:
    """
    Attributes:
        description (None | str): Document description.
        model_id (UUID): Base model the document is built on (the `modelId` supplied at create). Immutable — echoed here
            so a GET round-trips through PATCH; supplying a different value on PATCH is rejected.
        name (str): Document name.
        query_presentations (QueryPresentationsReadExternal): (Not statically modeled; use plain dicts.)
        workbook_model_id (UUID): Server-assigned WORKBOOK-layer model layered on `modelId`. Read-only — echoed here so
            a GET round-trips through PATCH; each draft has its own, so a draft read returns the draft workbook’s model.
        containers (list[ContainersItem] | Unset): Container layout array (grid / stack / page / reference containers,
            recursively nested). The server validates the full structure on apply. (Not statically modeled; use plain
            dicts.)
        controls (ControlsReadExternal | Unset): (Not statically modeled; use plain dicts.)
        settings (SettingsReadExternal | Unset):
    """

    description: None | str
    model_id: UUID
    name: str
    query_presentations: QueryPresentationsReadExternal
    workbook_model_id: UUID
    containers: list[ContainersItem] | Unset = UNSET
    controls: ControlsReadExternal | Unset = UNSET
    settings: SettingsReadExternal | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description: None | str
        description = self.description

        model_id = str(self.model_id)

        name = self.name

        query_presentations = self.query_presentations.to_dict()

        workbook_model_id = str(self.workbook_model_id)

        containers: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.containers, Unset):
            containers = []
            for componentsschemas_containers_item_data in self.containers:
                componentsschemas_containers_item = componentsschemas_containers_item_data.to_dict()
                containers.append(componentsschemas_containers_item)

        controls: dict[str, Any] | Unset = UNSET
        if not isinstance(self.controls, Unset):
            controls = self.controls.to_dict()

        settings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.settings, Unset):
            settings = self.settings.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "description": description,
                "modelId": model_id,
                "name": name,
                "queryPresentations": query_presentations,
                "workbookModelId": workbook_model_id,
            }
        )
        if containers is not UNSET:
            field_dict["containers"] = containers
        if controls is not UNSET:
            field_dict["controls"] = controls
        if settings is not UNSET:
            field_dict["settings"] = settings

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.containers_item import ContainersItem
        from ..models.controls_read_external import ControlsReadExternal
        from ..models.query_presentations_read_external import QueryPresentationsReadExternal
        from ..models.settings_read_external import SettingsReadExternal

        d = dict(src_dict)

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        model_id = UUID(d.pop("modelId"))

        name = d.pop("name")

        query_presentations = QueryPresentationsReadExternal.from_dict(d.pop("queryPresentations"))

        workbook_model_id = UUID(d.pop("workbookModelId"))

        _containers = d.pop("containers", UNSET)
        containers: list[ContainersItem] | Unset = UNSET
        if _containers is not UNSET:
            containers = []
            for componentsschemas_containers_item_data in _containers:
                componentsschemas_containers_item = ContainersItem.from_dict(componentsschemas_containers_item_data)

                containers.append(componentsschemas_containers_item)

        _controls = d.pop("controls", UNSET)
        controls: ControlsReadExternal | Unset
        if isinstance(_controls, Unset):
            controls = UNSET
        else:
            controls = ControlsReadExternal.from_dict(_controls)

        _settings = d.pop("settings", UNSET)
        settings: SettingsReadExternal | Unset
        if isinstance(_settings, Unset):
            settings = UNSET
        else:
            settings = SettingsReadExternal.from_dict(_settings)

        documents_v2_read_response = cls(
            description=description,
            model_id=model_id,
            name=name,
            query_presentations=query_presentations,
            workbook_model_id=workbook_model_id,
            containers=containers,
            controls=controls,
            settings=settings,
        )

        documents_v2_read_response.additional_properties = d
        return documents_v2_read_response

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

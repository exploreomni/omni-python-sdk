from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.controls_patch_external import ControlsPatchExternal
    from ..models.grid_container import GridContainer
    from ..models.page_container import PageContainer
    from ..models.query_presentations_patch_external import QueryPresentationsPatchExternal
    from ..models.settings_patch_external import SettingsPatchExternal
    from ..models.stack_container import StackContainer


T = TypeVar("T", bound="DocumentsV2CreateBody")


@_attrs_define
class DocumentsV2CreateBody:
    """
    Attributes:
        model_id (UUID): Base workbook model the document is built on — a SHARED model, or a SHARED_EXTENSION with
            `allowAsWorkbookBase = true`.
        name (str): Document name.
        containers (list[GridContainer | PageContainer | StackContainer] | None | Unset): Container layout array, or
            `null` to create a workbook-only document with no dashboard. When `null`, `controls` and `settings` must be
            omitted.
        controls (ControlsPatchExternal | Unset): (Not statically modeled; use plain dicts.)
        description (None | str | Unset): Document description.
        folder_id (None | Unset | UUID): Folder to create the document in. When omitted, defaults to the caller’s
            personal "My documents" (requires permission to save personal content — otherwise the request is rejected).
        identifier (str | Unset): Optional document identifier. If omitted, an identifier is auto-generated. Must be
            unique within the organization.
        query_presentations (QueryPresentationsPatchExternal | Unset): (Not statically modeled; use plain dicts.)
        settings (SettingsPatchExternal | Unset): Document settings. Shallow-merged with the existing settings.
        summary (str | Unset): Optional. Caller-supplied note describing the create, written to the history audit trail.
            When omitted, the server auto-fills it with "Created document".
    """

    model_id: UUID
    name: str
    containers: list[GridContainer | PageContainer | StackContainer] | None | Unset = UNSET
    controls: ControlsPatchExternal | Unset = UNSET
    description: None | str | Unset = UNSET
    folder_id: None | Unset | UUID = UNSET
    identifier: str | Unset = UNSET
    query_presentations: QueryPresentationsPatchExternal | Unset = UNSET
    settings: SettingsPatchExternal | Unset = UNSET
    summary: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.grid_container import GridContainer
        from ..models.page_container import PageContainer

        model_id = str(self.model_id)

        name = self.name

        containers: list[dict[str, Any]] | None | Unset
        if isinstance(self.containers, Unset):
            containers = UNSET
        elif isinstance(self.containers, list):
            containers = []
            for componentsschemas_containers_on_create_type_0_item_data in self.containers:
                componentsschemas_containers_on_create_type_0_item: dict[str, Any]
                if isinstance(componentsschemas_containers_on_create_type_0_item_data, GridContainer):
                    componentsschemas_containers_on_create_type_0_item = (
                        componentsschemas_containers_on_create_type_0_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_containers_on_create_type_0_item_data, PageContainer):
                    componentsschemas_containers_on_create_type_0_item = (
                        componentsschemas_containers_on_create_type_0_item_data.to_dict()
                    )
                else:
                    componentsschemas_containers_on_create_type_0_item = (
                        componentsschemas_containers_on_create_type_0_item_data.to_dict()
                    )

                containers.append(componentsschemas_containers_on_create_type_0_item)

        else:
            containers = self.containers

        controls: dict[str, Any] | Unset = UNSET
        if not isinstance(self.controls, Unset):
            controls = self.controls.to_dict()

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        folder_id: None | str | Unset
        if isinstance(self.folder_id, Unset):
            folder_id = UNSET
        elif isinstance(self.folder_id, UUID):
            folder_id = str(self.folder_id)
        else:
            folder_id = self.folder_id

        identifier = self.identifier

        query_presentations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query_presentations, Unset):
            query_presentations = self.query_presentations.to_dict()

        settings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.settings, Unset):
            settings = self.settings.to_dict()

        summary = self.summary

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "modelId": model_id,
                "name": name,
            }
        )
        if containers is not UNSET:
            field_dict["containers"] = containers
        if controls is not UNSET:
            field_dict["controls"] = controls
        if description is not UNSET:
            field_dict["description"] = description
        if folder_id is not UNSET:
            field_dict["folderId"] = folder_id
        if identifier is not UNSET:
            field_dict["identifier"] = identifier
        if query_presentations is not UNSET:
            field_dict["queryPresentations"] = query_presentations
        if settings is not UNSET:
            field_dict["settings"] = settings
        if summary is not UNSET:
            field_dict["summary"] = summary

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.controls_patch_external import ControlsPatchExternal
        from ..models.grid_container import GridContainer
        from ..models.page_container import PageContainer
        from ..models.query_presentations_patch_external import QueryPresentationsPatchExternal
        from ..models.settings_patch_external import SettingsPatchExternal
        from ..models.stack_container import StackContainer

        d = dict(src_dict)
        model_id = UUID(d.pop("modelId"))

        name = d.pop("name")

        def _parse_containers(data: object) -> list[GridContainer | PageContainer | StackContainer] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_containers_on_create_type_0 = []
                _componentsschemas_containers_on_create_type_0 = data
                for (
                    componentsschemas_containers_on_create_type_0_item_data
                ) in _componentsschemas_containers_on_create_type_0:

                    def _parse_componentsschemas_containers_on_create_type_0_item(
                        data: object,
                    ) -> GridContainer | PageContainer | StackContainer:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_containers_on_create_type_0_item_type_0 = GridContainer.from_dict(data)

                            return componentsschemas_containers_on_create_type_0_item_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_containers_on_create_type_0_item_type_1 = PageContainer.from_dict(data)

                            return componentsschemas_containers_on_create_type_0_item_type_1
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_containers_on_create_type_0_item_type_2 = StackContainer.from_dict(data)

                        return componentsschemas_containers_on_create_type_0_item_type_2

                    componentsschemas_containers_on_create_type_0_item = (
                        _parse_componentsschemas_containers_on_create_type_0_item(
                            componentsschemas_containers_on_create_type_0_item_data
                        )
                    )

                    componentsschemas_containers_on_create_type_0.append(
                        componentsschemas_containers_on_create_type_0_item
                    )

                return componentsschemas_containers_on_create_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[GridContainer | PageContainer | StackContainer] | None | Unset, data)

        containers = _parse_containers(d.pop("containers", UNSET))

        _controls = d.pop("controls", UNSET)
        controls: ControlsPatchExternal | Unset
        if isinstance(_controls, Unset):
            controls = UNSET
        else:
            controls = ControlsPatchExternal.from_dict(_controls)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        def _parse_folder_id(data: object) -> None | Unset | UUID:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                folder_id_type_0 = UUID(data)

                return folder_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | Unset | UUID, data)

        folder_id = _parse_folder_id(d.pop("folderId", UNSET))

        identifier = d.pop("identifier", UNSET)

        _query_presentations = d.pop("queryPresentations", UNSET)
        query_presentations: QueryPresentationsPatchExternal | Unset
        if isinstance(_query_presentations, Unset):
            query_presentations = UNSET
        else:
            query_presentations = QueryPresentationsPatchExternal.from_dict(_query_presentations)

        _settings = d.pop("settings", UNSET)
        settings: SettingsPatchExternal | Unset
        if isinstance(_settings, Unset):
            settings = UNSET
        else:
            settings = SettingsPatchExternal.from_dict(_settings)

        summary = d.pop("summary", UNSET)

        documents_v2_create_body = cls(
            model_id=model_id,
            name=name,
            containers=containers,
            controls=controls,
            description=description,
            folder_id=folder_id,
            identifier=identifier,
            query_presentations=query_presentations,
            settings=settings,
            summary=summary,
        )

        return documents_v2_create_body

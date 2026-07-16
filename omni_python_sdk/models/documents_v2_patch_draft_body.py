from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.containers_item import ContainersItem
    from ..models.controls_patch_external import ControlsPatchExternal
    from ..models.query_presentations_patch_external import QueryPresentationsPatchExternal
    from ..models.settings_patch_external import SettingsPatchExternal


T = TypeVar("T", bound="DocumentsV2PatchDraftBody")


@_attrs_define
class DocumentsV2PatchDraftBody:
    """
    Attributes:
        containers (list[ContainersItem] | Unset): Container layout array (grid / stack / page / reference containers,
            recursively nested). The server validates the full structure on apply. (Not statically modeled; use plain
            dicts.)
        controls (ControlsPatchExternal | Unset): (Not statically modeled; use plain dicts.)
        description (None | str | Unset):
        name (str | Unset): Document name.
        query_presentations (QueryPresentationsPatchExternal | Unset): (Not statically modeled; use plain dicts.)
        settings (SettingsPatchExternal | Unset): Document settings. Shallow-merged with the existing settings.
        summary (str | Unset): Optional. Caller-supplied description of what this patch changes, written to the history
            audit trail. When omitted, the server auto-generates one from the touched sections.
        model_id (UUID | Unset): The document's base model. Immutable and accepted only so a GET response round-trips
            through PATCH: a value matching the current model is a no-op, and a differing value is rejected — it cannot re-
            base the document. Omit it to leave the model untouched.
    """

    containers: list[ContainersItem] | Unset = UNSET
    controls: ControlsPatchExternal | Unset = UNSET
    description: None | str | Unset = UNSET
    name: str | Unset = UNSET
    query_presentations: QueryPresentationsPatchExternal | Unset = UNSET
    settings: SettingsPatchExternal | Unset = UNSET
    summary: str | Unset = UNSET
    model_id: UUID | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        containers: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.containers, Unset):
            containers = []
            for componentsschemas_containers_item_data in self.containers:
                componentsschemas_containers_item = componentsschemas_containers_item_data.to_dict()
                containers.append(componentsschemas_containers_item)

        controls: dict[str, Any] | Unset = UNSET
        if not isinstance(self.controls, Unset):
            controls = self.controls.to_dict()

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        name = self.name

        query_presentations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query_presentations, Unset):
            query_presentations = self.query_presentations.to_dict()

        settings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.settings, Unset):
            settings = self.settings.to_dict()

        summary = self.summary

        model_id: str | Unset = UNSET
        if not isinstance(self.model_id, Unset):
            model_id = str(self.model_id)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if containers is not UNSET:
            field_dict["containers"] = containers
        if controls is not UNSET:
            field_dict["controls"] = controls
        if description is not UNSET:
            field_dict["description"] = description
        if name is not UNSET:
            field_dict["name"] = name
        if query_presentations is not UNSET:
            field_dict["queryPresentations"] = query_presentations
        if settings is not UNSET:
            field_dict["settings"] = settings
        if summary is not UNSET:
            field_dict["summary"] = summary
        if model_id is not UNSET:
            field_dict["modelId"] = model_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.containers_item import ContainersItem
        from ..models.controls_patch_external import ControlsPatchExternal
        from ..models.query_presentations_patch_external import QueryPresentationsPatchExternal
        from ..models.settings_patch_external import SettingsPatchExternal

        d = dict(src_dict)
        _containers = d.pop("containers", UNSET)
        containers: list[ContainersItem] | Unset = UNSET
        if _containers is not UNSET:
            containers = []
            for componentsschemas_containers_item_data in _containers:
                componentsschemas_containers_item = ContainersItem.from_dict(componentsschemas_containers_item_data)

                containers.append(componentsschemas_containers_item)

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

        name = d.pop("name", UNSET)

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

        _model_id = d.pop("modelId", UNSET)
        model_id: UUID | Unset
        if isinstance(_model_id, Unset):
            model_id = UNSET
        else:
            model_id = UUID(_model_id)

        documents_v2_patch_draft_body = cls(
            containers=containers,
            controls=controls,
            description=description,
            name=name,
            query_presentations=query_presentations,
            settings=settings,
            summary=summary,
            model_id=model_id,
        )

        return documents_v2_patch_draft_body

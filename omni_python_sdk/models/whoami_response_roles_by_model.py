from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.whoami_model_role import WhoamiModelRole


T = TypeVar("T", bound="WhoamiResponseRolesByModel")


@_attrs_define
class WhoamiResponseRolesByModel:
    """Resolved role and effective permissions per model, keyed by model id. Connection role resolves per shared model, so
    this is per-model rather than a single global role.

    """

    additional_properties: dict[str, WhoamiModelRole] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.whoami_model_role import WhoamiModelRole

        d = dict(src_dict)
        whoami_response_roles_by_model = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = WhoamiModelRole.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        whoami_response_roles_by_model.additional_properties = additional_properties
        return whoami_response_roles_by_model

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> WhoamiModelRole:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: WhoamiModelRole) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.settings_patch_external_run_queries_on_type_1 import (
    SettingsPatchExternalRunQueriesOnType1,
    check_settings_patch_external_run_queries_on_type_1,
)
from ..models.settings_patch_external_run_queries_on_type_2_type_1 import (
    SettingsPatchExternalRunQueriesOnType2Type1,
    check_settings_patch_external_run_queries_on_type_2_type_1,
)
from ..models.settings_patch_external_run_queries_on_type_3_type_1 import (
    SettingsPatchExternalRunQueriesOnType3Type1,
    check_settings_patch_external_run_queries_on_type_3_type_1,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.settings_patch_external_custom_text_type_0 import SettingsPatchExternalCustomTextType0


T = TypeVar("T", bound="SettingsPatchExternal")


@_attrs_define
class SettingsPatchExternal:
    """Document settings. Shallow-merged with the existing settings.

    Attributes:
        crossfilter_enabled (bool | Unset): When true, clicking a value in one tile filters all other tiles on the
            dashboard.
        custom_text (None | SettingsPatchExternalCustomTextType0 | Unset): Custom text replacing default UI strings on
            the dashboard, e.g. when queries error or return no results.
        facet_filters (bool | Unset): When true, dashboard filters are applied per-facet when faceting is active.
        refresh_interval (float | None | Unset): Auto-refresh interval in seconds. Null disables auto-refresh.
        run_queries_on (None | SettingsPatchExternalRunQueriesOnType1 | SettingsPatchExternalRunQueriesOnType2Type1 |
            SettingsPatchExternalRunQueriesOnType3Type1 | Unset): Controls whether dashboard queries execute on the visible
            page or across all pages.
    """

    crossfilter_enabled: bool | Unset = UNSET
    custom_text: None | SettingsPatchExternalCustomTextType0 | Unset = UNSET
    facet_filters: bool | Unset = UNSET
    refresh_interval: float | None | Unset = UNSET
    run_queries_on: (
        None
        | SettingsPatchExternalRunQueriesOnType1
        | SettingsPatchExternalRunQueriesOnType2Type1
        | SettingsPatchExternalRunQueriesOnType3Type1
        | Unset
    ) = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.settings_patch_external_custom_text_type_0 import SettingsPatchExternalCustomTextType0

        crossfilter_enabled = self.crossfilter_enabled

        custom_text: dict[str, Any] | None | Unset
        if isinstance(self.custom_text, Unset):
            custom_text = UNSET
        elif isinstance(self.custom_text, SettingsPatchExternalCustomTextType0):
            custom_text = self.custom_text.to_dict()
        else:
            custom_text = self.custom_text

        facet_filters = self.facet_filters

        refresh_interval: float | None | Unset
        if isinstance(self.refresh_interval, Unset):
            refresh_interval = UNSET
        else:
            refresh_interval = self.refresh_interval

        run_queries_on: None | str | Unset
        if isinstance(self.run_queries_on, Unset):
            run_queries_on = UNSET
        elif isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        elif isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        elif isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        else:
            run_queries_on = self.run_queries_on

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if crossfilter_enabled is not UNSET:
            field_dict["crossfilterEnabled"] = crossfilter_enabled
        if custom_text is not UNSET:
            field_dict["customText"] = custom_text
        if facet_filters is not UNSET:
            field_dict["facetFilters"] = facet_filters
        if refresh_interval is not UNSET:
            field_dict["refreshInterval"] = refresh_interval
        if run_queries_on is not UNSET:
            field_dict["runQueriesOn"] = run_queries_on

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.settings_patch_external_custom_text_type_0 import SettingsPatchExternalCustomTextType0

        d = dict(src_dict)
        crossfilter_enabled = d.pop("crossfilterEnabled", UNSET)

        def _parse_custom_text(data: object) -> None | SettingsPatchExternalCustomTextType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                custom_text_type_0 = SettingsPatchExternalCustomTextType0.from_dict(data)

                return custom_text_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | SettingsPatchExternalCustomTextType0 | Unset, data)

        custom_text = _parse_custom_text(d.pop("customText", UNSET))

        facet_filters = d.pop("facetFilters", UNSET)

        def _parse_refresh_interval(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        refresh_interval = _parse_refresh_interval(d.pop("refreshInterval", UNSET))

        def _parse_run_queries_on(
            data: object,
        ) -> (
            None
            | SettingsPatchExternalRunQueriesOnType1
            | SettingsPatchExternalRunQueriesOnType2Type1
            | SettingsPatchExternalRunQueriesOnType3Type1
            | Unset
        ):
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_1 = check_settings_patch_external_run_queries_on_type_1(data)

                return run_queries_on_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_2_type_1 = check_settings_patch_external_run_queries_on_type_2_type_1(data)

                return run_queries_on_type_2_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_3_type_1 = check_settings_patch_external_run_queries_on_type_3_type_1(data)

                return run_queries_on_type_3_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                None
                | SettingsPatchExternalRunQueriesOnType1
                | SettingsPatchExternalRunQueriesOnType2Type1
                | SettingsPatchExternalRunQueriesOnType3Type1
                | Unset,
                data,
            )

        run_queries_on = _parse_run_queries_on(d.pop("runQueriesOn", UNSET))

        settings_patch_external = cls(
            crossfilter_enabled=crossfilter_enabled,
            custom_text=custom_text,
            facet_filters=facet_filters,
            refresh_interval=refresh_interval,
            run_queries_on=run_queries_on,
        )

        settings_patch_external.additional_properties = d
        return settings_patch_external

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

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.settings_read_external_run_queries_on_type_1 import (
    SettingsReadExternalRunQueriesOnType1,
    check_settings_read_external_run_queries_on_type_1,
)
from ..models.settings_read_external_run_queries_on_type_2_type_1 import (
    SettingsReadExternalRunQueriesOnType2Type1,
    check_settings_read_external_run_queries_on_type_2_type_1,
)
from ..models.settings_read_external_run_queries_on_type_3_type_1 import (
    SettingsReadExternalRunQueriesOnType3Type1,
    check_settings_read_external_run_queries_on_type_3_type_1,
)

if TYPE_CHECKING:
    from ..models.settings_read_external_custom_text_type_0 import SettingsReadExternalCustomTextType0


T = TypeVar("T", bound="SettingsReadExternal")


@_attrs_define
class SettingsReadExternal:
    """
    Attributes:
        crossfilter_enabled (bool): When true, clicking a value in one tile filters all other tiles on the dashboard.
        custom_text (None | SettingsReadExternalCustomTextType0): Custom text replacing default UI strings on the
            dashboard, e.g. when queries error or return no results.
        facet_filters (bool): When true, dashboard filters are applied per-facet when faceting is active.
        refresh_interval (float | None): Auto-refresh interval in seconds. Null disables auto-refresh.
        run_queries_on (None | SettingsReadExternalRunQueriesOnType1 | SettingsReadExternalRunQueriesOnType2Type1 |
            SettingsReadExternalRunQueriesOnType3Type1): Controls whether dashboard queries execute on the visible page or
            across all pages.
    """

    crossfilter_enabled: bool
    custom_text: None | SettingsReadExternalCustomTextType0
    facet_filters: bool
    refresh_interval: float | None
    run_queries_on: (
        None
        | SettingsReadExternalRunQueriesOnType1
        | SettingsReadExternalRunQueriesOnType2Type1
        | SettingsReadExternalRunQueriesOnType3Type1
    )
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.settings_read_external_custom_text_type_0 import SettingsReadExternalCustomTextType0

        crossfilter_enabled = self.crossfilter_enabled

        custom_text: dict[str, Any] | None
        if isinstance(self.custom_text, SettingsReadExternalCustomTextType0):
            custom_text = self.custom_text.to_dict()
        else:
            custom_text = self.custom_text

        facet_filters = self.facet_filters

        refresh_interval: float | None
        refresh_interval = self.refresh_interval

        run_queries_on: None | str
        if isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        elif isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        elif isinstance(self.run_queries_on, str):
            run_queries_on = self.run_queries_on
        else:
            run_queries_on = self.run_queries_on

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "crossfilterEnabled": crossfilter_enabled,
                "customText": custom_text,
                "facetFilters": facet_filters,
                "refreshInterval": refresh_interval,
                "runQueriesOn": run_queries_on,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.settings_read_external_custom_text_type_0 import SettingsReadExternalCustomTextType0

        d = dict(src_dict)
        crossfilter_enabled = d.pop("crossfilterEnabled")

        def _parse_custom_text(data: object) -> None | SettingsReadExternalCustomTextType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                custom_text_type_0 = SettingsReadExternalCustomTextType0.from_dict(data)

                return custom_text_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | SettingsReadExternalCustomTextType0, data)

        custom_text = _parse_custom_text(d.pop("customText"))

        facet_filters = d.pop("facetFilters")

        def _parse_refresh_interval(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        refresh_interval = _parse_refresh_interval(d.pop("refreshInterval"))

        def _parse_run_queries_on(
            data: object,
        ) -> (
            None
            | SettingsReadExternalRunQueriesOnType1
            | SettingsReadExternalRunQueriesOnType2Type1
            | SettingsReadExternalRunQueriesOnType3Type1
        ):
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_1 = check_settings_read_external_run_queries_on_type_1(data)

                return run_queries_on_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_2_type_1 = check_settings_read_external_run_queries_on_type_2_type_1(data)

                return run_queries_on_type_2_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                run_queries_on_type_3_type_1 = check_settings_read_external_run_queries_on_type_3_type_1(data)

                return run_queries_on_type_3_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                None
                | SettingsReadExternalRunQueriesOnType1
                | SettingsReadExternalRunQueriesOnType2Type1
                | SettingsReadExternalRunQueriesOnType3Type1,
                data,
            )

        run_queries_on = _parse_run_queries_on(d.pop("runQueriesOn"))

        settings_read_external = cls(
            crossfilter_enabled=crossfilter_enabled,
            custom_text=custom_text,
            facet_filters=facet_filters,
            refresh_interval=refresh_interval,
            run_queries_on=run_queries_on,
        )

        settings_read_external.additional_properties = d
        return settings_read_external

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

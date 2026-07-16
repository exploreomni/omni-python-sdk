from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.dashboards_download_body_format import DashboardsDownloadBodyFormat, check_dashboards_download_body_format
from ..models.dashboards_download_body_paper_format import (
    DashboardsDownloadBodyPaperFormat,
    check_dashboards_download_body_paper_format,
)
from ..models.dashboards_download_body_paper_orientation import (
    DashboardsDownloadBodyPaperOrientation,
    check_dashboards_download_body_paper_orientation,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="DashboardsDownloadBody")


@_attrs_define
class DashboardsDownloadBody:
    """
    Attributes:
        format_ (DashboardsDownloadBodyFormat): Output format for the download: pdf, png, csv, xlsx, or json Example:
            pdf.
        enable_formatting (bool | Unset): Compatible with csv, xlsx & json formats. If true, formatting will be enabled
            in the output. Note: If true for json format, a queryIdentifierMapKey is required. Default: False.
        expand_tables_to_show_all_rows (bool | Unset): Compatible with pdf and png formats. If true, up to 1,000 rows in
            table visualizations will be included in the delivery. Note: This parameter cannot be used when paperFormat:
            fit_page.
        filter_config (Any | Unset): An object specifying the filter conditions to apply to the task. The filter key
            specified must already exist in the dashboard. Example: {'status': ['active', 'pending']}.
        hide_hidden_fields (bool | Unset): Compatible with csv & xlsx formats. If true, fields marked as hidden won't be
            displayed in the output. Default: False.
        hide_title (bool | Unset): Compatible with pdf & png formats. If true, the content's title will be hidden in the
            output. Default: False.
        max_row_limit (float | Unset): Compatible with csv, json, & xlsx formats. Used with overrideRowLimit. Specifies
            the maximum number of rows. Example: 1000.
        override_row_limit (bool | Unset): Compatible with csv, json, & xlsx formats. If true, the default row limit
            will be overridden. Note: If true for json and xlsx formats, a queryIdentifierMapKey is required. Default:
            False.
        paper_format (DashboardsDownloadBodyPaperFormat | Unset): Compatible with pdf formats. Defines the paper format
            (size) of the resulting PDF. Must be one of: a3, a4, letter, legal, fit_page, tabloid. Example: letter.
        paper_orientation (DashboardsDownloadBodyPaperOrientation | Unset): Compatible with pdf formats. Defines the
            paper orientation of the resulting PDF. Must be one of: portrait, landscape. Example: landscape.
        query_identifier_map_key (str | Unset): Required for single tile tasks. The ID of the query to include in a
            single tile task. Must reference a valid query in the dashboard. Example: Jmn2r3KV.
        show_content_link (bool | Unset): Compatible with all formats except link_only. If true, a link to the content
            will be shown in the output. Default: True. Example: True.
        show_filters (bool | Unset): Compatible with all formats except link_only & csv. If true, filters will be shown
            in the output. Default: True. Example: True.
        single_column_layout (bool | Unset): Compatible with pdf and png formats. If true, dashboard tiles will be
            arranged into a single vertical column.
        use_cache (bool | Unset): If true, allow scheduled queries to use cached results instead of always running fresh
            queries. Default: False.
        filename (str | Unset): Custom filename for the downloaded file (without extension) Example: monthly-report.
    """

    format_: DashboardsDownloadBodyFormat
    enable_formatting: bool | Unset = False
    expand_tables_to_show_all_rows: bool | Unset = UNSET
    filter_config: Any | Unset = UNSET
    hide_hidden_fields: bool | Unset = False
    hide_title: bool | Unset = False
    max_row_limit: float | Unset = UNSET
    override_row_limit: bool | Unset = False
    paper_format: DashboardsDownloadBodyPaperFormat | Unset = UNSET
    paper_orientation: DashboardsDownloadBodyPaperOrientation | Unset = UNSET
    query_identifier_map_key: str | Unset = UNSET
    show_content_link: bool | Unset = True
    show_filters: bool | Unset = True
    single_column_layout: bool | Unset = UNSET
    use_cache: bool | Unset = False
    filename: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        format_: str = self.format_

        enable_formatting = self.enable_formatting

        expand_tables_to_show_all_rows = self.expand_tables_to_show_all_rows

        filter_config = self.filter_config

        hide_hidden_fields = self.hide_hidden_fields

        hide_title = self.hide_title

        max_row_limit = self.max_row_limit

        override_row_limit = self.override_row_limit

        paper_format: str | Unset = UNSET
        if not isinstance(self.paper_format, Unset):
            paper_format = self.paper_format

        paper_orientation: str | Unset = UNSET
        if not isinstance(self.paper_orientation, Unset):
            paper_orientation = self.paper_orientation

        query_identifier_map_key = self.query_identifier_map_key

        show_content_link = self.show_content_link

        show_filters = self.show_filters

        single_column_layout = self.single_column_layout

        use_cache = self.use_cache

        filename = self.filename

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "format": format_,
            }
        )
        if enable_formatting is not UNSET:
            field_dict["enableFormatting"] = enable_formatting
        if expand_tables_to_show_all_rows is not UNSET:
            field_dict["expandTablesToShowAllRows"] = expand_tables_to_show_all_rows
        if filter_config is not UNSET:
            field_dict["filterConfig"] = filter_config
        if hide_hidden_fields is not UNSET:
            field_dict["hideHiddenFields"] = hide_hidden_fields
        if hide_title is not UNSET:
            field_dict["hideTitle"] = hide_title
        if max_row_limit is not UNSET:
            field_dict["maxRowLimit"] = max_row_limit
        if override_row_limit is not UNSET:
            field_dict["overrideRowLimit"] = override_row_limit
        if paper_format is not UNSET:
            field_dict["paperFormat"] = paper_format
        if paper_orientation is not UNSET:
            field_dict["paperOrientation"] = paper_orientation
        if query_identifier_map_key is not UNSET:
            field_dict["queryIdentifierMapKey"] = query_identifier_map_key
        if show_content_link is not UNSET:
            field_dict["showContentLink"] = show_content_link
        if show_filters is not UNSET:
            field_dict["showFilters"] = show_filters
        if single_column_layout is not UNSET:
            field_dict["singleColumnLayout"] = single_column_layout
        if use_cache is not UNSET:
            field_dict["useCache"] = use_cache
        if filename is not UNSET:
            field_dict["filename"] = filename

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        format_ = check_dashboards_download_body_format(d.pop("format"))

        enable_formatting = d.pop("enableFormatting", UNSET)

        expand_tables_to_show_all_rows = d.pop("expandTablesToShowAllRows", UNSET)

        filter_config = d.pop("filterConfig", UNSET)

        hide_hidden_fields = d.pop("hideHiddenFields", UNSET)

        hide_title = d.pop("hideTitle", UNSET)

        max_row_limit = d.pop("maxRowLimit", UNSET)

        override_row_limit = d.pop("overrideRowLimit", UNSET)

        _paper_format = d.pop("paperFormat", UNSET)
        paper_format: DashboardsDownloadBodyPaperFormat | Unset
        if isinstance(_paper_format, Unset):
            paper_format = UNSET
        else:
            paper_format = check_dashboards_download_body_paper_format(_paper_format)

        _paper_orientation = d.pop("paperOrientation", UNSET)
        paper_orientation: DashboardsDownloadBodyPaperOrientation | Unset
        if isinstance(_paper_orientation, Unset):
            paper_orientation = UNSET
        else:
            paper_orientation = check_dashboards_download_body_paper_orientation(_paper_orientation)

        query_identifier_map_key = d.pop("queryIdentifierMapKey", UNSET)

        show_content_link = d.pop("showContentLink", UNSET)

        show_filters = d.pop("showFilters", UNSET)

        single_column_layout = d.pop("singleColumnLayout", UNSET)

        use_cache = d.pop("useCache", UNSET)

        filename = d.pop("filename", UNSET)

        dashboards_download_body = cls(
            format_=format_,
            enable_formatting=enable_formatting,
            expand_tables_to_show_all_rows=expand_tables_to_show_all_rows,
            filter_config=filter_config,
            hide_hidden_fields=hide_hidden_fields,
            hide_title=hide_title,
            max_row_limit=max_row_limit,
            override_row_limit=override_row_limit,
            paper_format=paper_format,
            paper_orientation=paper_orientation,
            query_identifier_map_key=query_identifier_map_key,
            show_content_link=show_content_link,
            show_filters=show_filters,
            single_column_layout=single_column_layout,
            use_cache=use_cache,
            filename=filename,
        )

        return dashboards_download_body

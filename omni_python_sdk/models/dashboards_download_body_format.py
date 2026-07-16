from typing import Literal

DashboardsDownloadBodyFormat = Literal["csv", "json", "pdf", "png", "xlsx"]

DASHBOARDS_DOWNLOAD_BODY_FORMAT_VALUES: set[DashboardsDownloadBodyFormat] = {
    "csv",
    "json",
    "pdf",
    "png",
    "xlsx",
}


def check_dashboards_download_body_format(value: str) -> DashboardsDownloadBodyFormat:
    if value in DASHBOARDS_DOWNLOAD_BODY_FORMAT_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DASHBOARDS_DOWNLOAD_BODY_FORMAT_VALUES!r}")

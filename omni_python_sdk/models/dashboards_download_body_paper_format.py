from typing import Literal

DashboardsDownloadBodyPaperFormat = Literal["a3", "a4", "fit_page", "legal", "letter", "tabloid"]

DASHBOARDS_DOWNLOAD_BODY_PAPER_FORMAT_VALUES: set[DashboardsDownloadBodyPaperFormat] = {
    "a3",
    "a4",
    "fit_page",
    "legal",
    "letter",
    "tabloid",
}


def check_dashboards_download_body_paper_format(value: str) -> DashboardsDownloadBodyPaperFormat:
    if value in DASHBOARDS_DOWNLOAD_BODY_PAPER_FORMAT_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DASHBOARDS_DOWNLOAD_BODY_PAPER_FORMAT_VALUES!r}")

from typing import Literal

DashboardsDownloadBodyPaperOrientation = Literal["landscape", "portrait"]

DASHBOARDS_DOWNLOAD_BODY_PAPER_ORIENTATION_VALUES: set[DashboardsDownloadBodyPaperOrientation] = {
    "landscape",
    "portrait",
}


def check_dashboards_download_body_paper_orientation(value: str) -> DashboardsDownloadBodyPaperOrientation:
    if value in DASHBOARDS_DOWNLOAD_BODY_PAPER_ORIENTATION_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {DASHBOARDS_DOWNLOAD_BODY_PAPER_ORIENTATION_VALUES!r}"
    )

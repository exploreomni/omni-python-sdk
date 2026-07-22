from typing import Literal

DocumentsPutQueryPresentationChartTypeType2Type1 = Literal[
    "area",
    "areaStacked",
    "areaStackedPercentage",
    "auto",
    "bar",
    "barGrouped",
    "barLine",
    "barStacked",
    "barStackedPercentage",
    "boxplot",
    "code",
    "column",
    "columnGrouped",
    "columnStacked",
    "columnStackedPercentage",
    "funnel",
    "heatmap",
    "kpi",
    "line",
    "lineColor",
    "map",
    "markdown",
    "omni-ai-summary-markdown",
    "omni-spreadsheet",
    "pie",
    "point",
    "pointColor",
    "pointSize",
    "pointSizeColor",
    "regionMap",
    "sankey",
    "singleRecord",
    "summaryValue",
    "svgMap",
    "table",
    "treemap",
]

DOCUMENTS_PUT_QUERY_PRESENTATION_CHART_TYPE_TYPE_2_TYPE_1_VALUES: set[
    DocumentsPutQueryPresentationChartTypeType2Type1
] = {
    "area",
    "areaStacked",
    "areaStackedPercentage",
    "auto",
    "bar",
    "barGrouped",
    "barLine",
    "barStacked",
    "barStackedPercentage",
    "boxplot",
    "code",
    "column",
    "columnGrouped",
    "columnStacked",
    "columnStackedPercentage",
    "funnel",
    "heatmap",
    "kpi",
    "line",
    "lineColor",
    "map",
    "markdown",
    "omni-ai-summary-markdown",
    "omni-spreadsheet",
    "pie",
    "point",
    "pointColor",
    "pointSize",
    "pointSizeColor",
    "regionMap",
    "sankey",
    "singleRecord",
    "summaryValue",
    "svgMap",
    "table",
    "treemap",
}


def check_documents_put_query_presentation_chart_type_type_2_type_1(
    value: str,
) -> DocumentsPutQueryPresentationChartTypeType2Type1:
    if value in DOCUMENTS_PUT_QUERY_PRESENTATION_CHART_TYPE_TYPE_2_TYPE_1_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {DOCUMENTS_PUT_QUERY_PRESENTATION_CHART_TYPE_TYPE_2_TYPE_1_VALUES!r}"
    )

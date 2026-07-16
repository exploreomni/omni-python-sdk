from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ai_job_action_query_result_status import (
    AiJobActionQueryResultStatus,
    check_ai_job_action_query_result_status,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ai_job_action_query_result_query import AiJobActionQueryResultQuery


T = TypeVar("T", bound="AiJobActionQueryResult")


@_attrs_define
class AiJobActionQueryResult:
    """Query result data. Only present for generate_query action types.

    Attributes:
        csv_result (str): Query results formatted as CSV text. Example: Name,Total Revenue
            Ray-Ban Sunglasses,"678,994.41"
            Levi's 501 Jeans,"475,072.00".
        csv_result_was_truncated (bool): Whether the CSV data was truncated due to size limits. If true, the full result
            set may contain additional rows not included in csvResult.
        has_results (bool): Whether the query returned any data rows. Example: True.
        query (AiJobActionQueryResultQuery): The semantic query definition that was executed. This can be used with the
            POST /api/v1/query/run endpoint to re-run the query.
        query_name (str): Human-readable name describing what this query retrieves. Example: Top 5 Products by Revenue.
        status (AiJobActionQueryResultStatus): Whether the query executed successfully. Example: success.
        total_row_count (int): Total number of rows returned by the query. Example: 5.
        result_id (str | Unset): Stable, unique identifier for this query result within the job. Use it to reference a
            specific result — for example, to correlate or de-duplicate results across responses. Example:
            928c5838-000d-4943-b305-f6242c1b4922.
    """

    csv_result: str
    csv_result_was_truncated: bool
    has_results: bool
    query: AiJobActionQueryResultQuery
    query_name: str
    status: AiJobActionQueryResultStatus
    total_row_count: int
    result_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        csv_result = self.csv_result

        csv_result_was_truncated = self.csv_result_was_truncated

        has_results = self.has_results

        query = self.query.to_dict()

        query_name = self.query_name

        status: str = self.status

        total_row_count = self.total_row_count

        result_id = self.result_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "csvResult": csv_result,
                "csvResultWasTruncated": csv_result_was_truncated,
                "hasResults": has_results,
                "query": query,
                "queryName": query_name,
                "status": status,
                "totalRowCount": total_row_count,
            }
        )
        if result_id is not UNSET:
            field_dict["resultId"] = result_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_job_action_query_result_query import AiJobActionQueryResultQuery

        d = dict(src_dict)
        csv_result = d.pop("csvResult")

        csv_result_was_truncated = d.pop("csvResultWasTruncated")

        has_results = d.pop("hasResults")

        query = AiJobActionQueryResultQuery.from_dict(d.pop("query"))

        query_name = d.pop("queryName")

        status = check_ai_job_action_query_result_status(d.pop("status"))

        total_row_count = d.pop("totalRowCount")

        result_id = d.pop("resultId", UNSET)

        ai_job_action_query_result = cls(
            csv_result=csv_result,
            csv_result_was_truncated=csv_result_was_truncated,
            has_results=has_results,
            query=query,
            query_name=query_name,
            status=status,
            total_row_count=total_row_count,
            result_id=result_id,
        )

        ai_job_action_query_result.additional_properties = d
        return ai_job_action_query_result

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

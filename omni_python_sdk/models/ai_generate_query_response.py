from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ai_generate_query_response_error_type_0 import AiGenerateQueryResponseErrorType0
    from ..models.ai_generate_query_response_result import AiGenerateQueryResponseResult
    from ..models.ai_semantic_query import AiSemanticQuery


T = TypeVar("T", bound="AiGenerateQueryResponse")


@_attrs_define
class AiGenerateQueryResponse:
    """
    Attributes:
        error (AiGenerateQueryResponseErrorType0 | None): Error details if query generation failed. Null on success.
        query (AiSemanticQuery): The generated semantic query definition. Null if generation failed. This query can be
            passed directly to the POST /api/v1/query/run endpoint. (Not statically modeled; use plain dicts.)
        base_view (None | str | Unset): The base view name used for query generation when queryAllViews surfaced a non-
            topic view. Mutually exclusive with `topic` — exactly one is non-null when a query was generated.
        downgraded_model_tier (str | Unset): Present only when the organization is over its AI downgrade threshold,
            signaling the query was generated on a downgraded (cheaper) model tier (e.g. 'haiku') to conserve credits.
            Advisory and best-effort — the call still succeeds, and clients may surface that a downgraded model was used.
            Absent when no downgrade applied. Example: haiku.
        result (AiGenerateQueryResponseResult | Unset): Query execution results as a JSON object. Only present when
            runQuery is true (the default) and the query executed successfully. The structure contains the query result
            data.
        topic (None | str | Unset): The topic name used for query generation. Mutually exclusive with `baseView` —
            exactly one is non-null when a query was generated. Example: order_items.
        workbook_url (str | Unset): URL to view and edit the generated query in an Omni workbook. Only present when
            workbookUrl was set to true in the request. Example: https://myorg.omni.co/w/abc123/1.
    """

    error: AiGenerateQueryResponseErrorType0 | None
    query: AiSemanticQuery
    base_view: None | str | Unset = UNSET
    downgraded_model_tier: str | Unset = UNSET
    result: AiGenerateQueryResponseResult | Unset = UNSET
    topic: None | str | Unset = UNSET
    workbook_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.ai_generate_query_response_error_type_0 import AiGenerateQueryResponseErrorType0

        error: dict[str, Any] | None
        if isinstance(self.error, AiGenerateQueryResponseErrorType0):
            error = self.error.to_dict()
        else:
            error = self.error

        query = self.query.to_dict()

        base_view: None | str | Unset
        if isinstance(self.base_view, Unset):
            base_view = UNSET
        else:
            base_view = self.base_view

        downgraded_model_tier = self.downgraded_model_tier

        result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.result, Unset):
            result = self.result.to_dict()

        topic: None | str | Unset
        if isinstance(self.topic, Unset):
            topic = UNSET
        else:
            topic = self.topic

        workbook_url = self.workbook_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
                "query": query,
            }
        )
        if base_view is not UNSET:
            field_dict["baseView"] = base_view
        if downgraded_model_tier is not UNSET:
            field_dict["downgradedModelTier"] = downgraded_model_tier
        if result is not UNSET:
            field_dict["result"] = result
        if topic is not UNSET:
            field_dict["topic"] = topic
        if workbook_url is not UNSET:
            field_dict["workbookUrl"] = workbook_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ai_generate_query_response_error_type_0 import AiGenerateQueryResponseErrorType0
        from ..models.ai_generate_query_response_result import AiGenerateQueryResponseResult
        from ..models.ai_semantic_query import AiSemanticQuery

        d = dict(src_dict)

        def _parse_error(data: object) -> AiGenerateQueryResponseErrorType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                error_type_0 = AiGenerateQueryResponseErrorType0.from_dict(data)

                return error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(AiGenerateQueryResponseErrorType0 | None, data)

        error = _parse_error(d.pop("error"))

        query = AiSemanticQuery.from_dict(d.pop("query"))

        def _parse_base_view(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        base_view = _parse_base_view(d.pop("baseView", UNSET))

        downgraded_model_tier = d.pop("downgradedModelTier", UNSET)

        _result = d.pop("result", UNSET)
        result: AiGenerateQueryResponseResult | Unset
        if isinstance(_result, Unset):
            result = UNSET
        else:
            result = AiGenerateQueryResponseResult.from_dict(_result)

        def _parse_topic(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        topic = _parse_topic(d.pop("topic", UNSET))

        workbook_url = d.pop("workbookUrl", UNSET)

        ai_generate_query_response = cls(
            error=error,
            query=query,
            base_view=base_view,
            downgraded_model_tier=downgraded_model_tier,
            result=result,
            topic=topic,
            workbook_url=workbook_url,
        )

        ai_generate_query_response.additional_properties = d
        return ai_generate_query_response

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

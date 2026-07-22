from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.eval_run_result_agentic_job import EvalRunResultAgenticJob


T = TypeVar("T", bound="EvalRunResult")


@_attrs_define
class EvalRunResult:
    """
    Attributes:
        agentic_job (EvalRunResultAgenticJob):
        ai_timing_ms (int | None): Strict main-agent LLM processing time in milliseconds — the measured model-call
            duration, excluding tool execution and subagent model calls (those count toward `tool_timing_ms`). Shown as "AI
            time" in the UI. Runs recorded before this was measured fall back to an approximation (`timing_ms` minus tool
            latency). Example: 4121.
        cost (float | None): Total LLM cost (USD) for this prompt, if available. Example: 0.0021.
        error_reason (None | str): Failure reason string for prompts whose underlying job failed.
        eval_prompt_id (None | UUID): Snapshot of the prompt id this result was executed for — repeated executions of
            the same prompt share it, and it survives later prompt deletion. Null on runs created before repeats existed.
            Example: bb0e8400-e29b-41d4-a716-446655440007.
        expectation (None | str): The prompt's expectation as of run creation (snapshotted, so later prompt edits don't
            change past runs), or null when none was set. The analysis judge scores the analysis against it. Example: The
            top product by revenue should be Aniseed Syrup..
        id (UUID): Unique identifier for the run result row. Example: aa0e8400-e29b-41d4-a716-446655440005.
        prompt (str): The prompt text that was evaluated. Example: What are the top 5 products by revenue?.
        query_count (int | None): Number of warehouse queries the underlying job ran. Null for runs executed before this
            metric was recorded. Example: 4.
        query_timing_ms (int | None): Total wall-clock time (milliseconds) the underlying job spent running warehouse
            queries — a proxy for query execution time. Null for runs executed before this metric was recorded. Example:
            1800.
        repeat_index (int | None): 0-based repeat number of this execution within the run (see the run's
            `repeat_count`). Null on runs created before repeats existed.
        score (float | None): Numeric judge score for this prompt result, if scoring ran. Example: 0.9.
        scoring_cost (float | None): Total LLM cost (USD) for scoring this prompt result. Example: 0.0004.
        timing_ms (int | None): Total `/generate` wall-time in milliseconds — LLM processing plus inner-loop tool
            execution. `ai_timing_ms` and `tool_timing_ms` split this; warehouse query time is separate (`query_timing_ms`).
            Example: 4321.
        tool_timing_ms (int | None): Inner-loop tool latency in milliseconds — time spent running tools the model
            invoked (model and field-value lookups, query planning), excluding the warehouse query itself
            (`query_timing_ms`). Null for runs recorded before per-tool latency was tracked. Example: 200.
    """

    agentic_job: EvalRunResultAgenticJob
    ai_timing_ms: int | None
    cost: float | None
    error_reason: None | str
    eval_prompt_id: None | UUID
    expectation: None | str
    id: UUID
    prompt: str
    query_count: int | None
    query_timing_ms: int | None
    repeat_index: int | None
    score: float | None
    scoring_cost: float | None
    timing_ms: int | None
    tool_timing_ms: int | None
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        agentic_job = self.agentic_job.to_dict()

        ai_timing_ms: int | None
        ai_timing_ms = self.ai_timing_ms

        cost: float | None
        cost = self.cost

        error_reason: None | str
        error_reason = self.error_reason

        eval_prompt_id: None | str
        if isinstance(self.eval_prompt_id, UUID):
            eval_prompt_id = str(self.eval_prompt_id)
        else:
            eval_prompt_id = self.eval_prompt_id

        expectation: None | str
        expectation = self.expectation

        id = str(self.id)

        prompt = self.prompt

        query_count: int | None
        query_count = self.query_count

        query_timing_ms: int | None
        query_timing_ms = self.query_timing_ms

        repeat_index: int | None
        repeat_index = self.repeat_index

        score: float | None
        score = self.score

        scoring_cost: float | None
        scoring_cost = self.scoring_cost

        timing_ms: int | None
        timing_ms = self.timing_ms

        tool_timing_ms: int | None
        tool_timing_ms = self.tool_timing_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "agentic_job": agentic_job,
                "ai_timing_ms": ai_timing_ms,
                "cost": cost,
                "error_reason": error_reason,
                "eval_prompt_id": eval_prompt_id,
                "expectation": expectation,
                "id": id,
                "prompt": prompt,
                "query_count": query_count,
                "query_timing_ms": query_timing_ms,
                "repeat_index": repeat_index,
                "score": score,
                "scoring_cost": scoring_cost,
                "timing_ms": timing_ms,
                "tool_timing_ms": tool_timing_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_run_result_agentic_job import EvalRunResultAgenticJob

        d = dict(src_dict)
        agentic_job = EvalRunResultAgenticJob.from_dict(d.pop("agentic_job"))

        def _parse_ai_timing_ms(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        ai_timing_ms = _parse_ai_timing_ms(d.pop("ai_timing_ms"))

        def _parse_cost(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        cost = _parse_cost(d.pop("cost"))

        def _parse_error_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        error_reason = _parse_error_reason(d.pop("error_reason"))

        def _parse_eval_prompt_id(data: object) -> None | UUID:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                eval_prompt_id_type_0 = UUID(data)

                return eval_prompt_id_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UUID, data)

        eval_prompt_id = _parse_eval_prompt_id(d.pop("eval_prompt_id"))

        def _parse_expectation(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        expectation = _parse_expectation(d.pop("expectation"))

        id = UUID(d.pop("id"))

        prompt = d.pop("prompt")

        def _parse_query_count(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        query_count = _parse_query_count(d.pop("query_count"))

        def _parse_query_timing_ms(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        query_timing_ms = _parse_query_timing_ms(d.pop("query_timing_ms"))

        def _parse_repeat_index(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        repeat_index = _parse_repeat_index(d.pop("repeat_index"))

        def _parse_score(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        score = _parse_score(d.pop("score"))

        def _parse_scoring_cost(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        scoring_cost = _parse_scoring_cost(d.pop("scoring_cost"))

        def _parse_timing_ms(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        timing_ms = _parse_timing_ms(d.pop("timing_ms"))

        def _parse_tool_timing_ms(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        tool_timing_ms = _parse_tool_timing_ms(d.pop("tool_timing_ms"))

        eval_run_result = cls(
            agentic_job=agentic_job,
            ai_timing_ms=ai_timing_ms,
            cost=cost,
            error_reason=error_reason,
            eval_prompt_id=eval_prompt_id,
            expectation=expectation,
            id=id,
            prompt=prompt,
            query_count=query_count,
            query_timing_ms=query_timing_ms,
            repeat_index=repeat_index,
            score=score,
            scoring_cost=scoring_cost,
            timing_ms=timing_ms,
            tool_timing_ms=tool_timing_ms,
        )

        eval_run_result.additional_properties = d
        return eval_run_result

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

from typing import Literal

EvalRunListItemStatus = Literal["CANCELLED", "COMPLETE", "RUNNING"]

EVAL_RUN_LIST_ITEM_STATUS_VALUES: set[EvalRunListItemStatus] = {
    "CANCELLED",
    "COMPLETE",
    "RUNNING",
}


def check_eval_run_list_item_status(value: str) -> EvalRunListItemStatus:
    if value in EVAL_RUN_LIST_ITEM_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {EVAL_RUN_LIST_ITEM_STATUS_VALUES!r}")

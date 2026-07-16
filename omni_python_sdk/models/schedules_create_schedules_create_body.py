from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.schedules_create_schedules_create_body_condition_type import (
    SchedulesCreateSchedulesCreateBodyConditionType,
    check_schedules_create_schedules_create_body_condition_type,
)
from ..models.schedules_create_schedules_create_body_destination_type import (
    SchedulesCreateSchedulesCreateBodyDestinationType,
    check_schedules_create_schedules_create_body_destination_type,
)
from ..models.schedules_create_schedules_create_body_format import (
    SchedulesCreateSchedulesCreateBodyFormat,
    check_schedules_create_schedules_create_body_format,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.schedules_create_schedules_create_body_recipients_item import (
        SchedulesCreateSchedulesCreateBodyRecipientsItem,
    )


T = TypeVar("T", bound="SchedulesCreateSchedulesCreateBody")


@_attrs_define
class SchedulesCreateSchedulesCreateBody:
    """Request body for creating a scheduled task. Required fields vary by destinationType.

    Attributes:
        destination_type (SchedulesCreateSchedulesCreateBodyDestinationType): The delivery destination type Example:
            email.
        format_ (SchedulesCreateSchedulesCreateBodyFormat): The output format: link_only, pdf, png, csv, xlsx, json
            Example: pdf.
        identifier (str): The ID of the dashboard to schedule Example: 12db1a0a.
        name (str): The name of the scheduled task Example: Weekly Sales Report.
        schedule (str): AWS EventBridge cron expression (minute hour day-of-month month day-of-week year) Example: 0 9 ?
            * MON *.
        timezone (str): IANA timezone for the schedule Example: America/New_York.
        bucket_name (str | Unset): S3 bucket name (S3 destination only). Must be 3-63 characters, lowercase. Example:
            my-reports-bucket.
        condition_query_map_key (str | Unset): The ID of the query to monitor for triggering an alert. Required if
            conditionType is provided. Example: Jmn2r3KV.
        condition_type (SchedulesCreateSchedulesCreateBodyConditionType | Unset): Defines the type of condition to use
            for alerts. Required if conditionQueryMapKey is provided. Example: RESULTS_PRESENT.
        enable_formatting (bool | Unset): If true, formatting will be enabled in the output
        fan_out (bool | Unset): If true, send personalized emails to each recipient (email only)
        filter_config (Any | Unset): Filter conditions to apply to the task Example: {'status': ['active', 'pending']}.
        hide_hidden_fields (bool | Unset): If true, hidden fields won't be displayed (csv/xlsx only)
        hide_title (bool | Unset): If true, hide the title in output (pdf/png only)
        key_prefix (str | Unset): S3 key prefix / folder path (S3 destination only). Leading slashes are normalized.
            Example: reports/weekly/.
        kill_jobs_on_failure (bool | Unset): If true, stop entire job if any queries fail
        recipients (list[SchedulesCreateSchedulesCreateBodyRecipientsItem] | Unset): Email recipients (email destination
            only). For Slack destinations, use the "recipients" field with a channel ID string or user ID(s) as a string or
            array.
        region (str | Unset): AWS region where the S3 bucket is located (S3 destination only). Example: us-east-1.
        role_arn (str | Unset): ARN of the cross-account IAM role Omni will assume to write to the S3 bucket (S3
            destination only). Example: arn:aws:iam::123456789012:role/OmniS3DeliveryRole.
        show_content_link (bool | Unset): If true, include a link to the content Example: True.
        show_filters (bool | Unset): If true, show applied filters in output Example: True.
        slack_recipient_type (str | Unset): Slack recipient type (Slack destination only). Use "channel" to deliver to a
            single Slack channel, or "users" to deliver to one or more Slack users via direct message. Example: channel.
        test_now (bool | Unset): If true, run immediately instead of scheduling
        timezone_override (None | str | Unset): Optional IANA timezone applied to query execution at render time.
            Distinct from `timezone` (which controls *when* the schedule fires). Omit or pass null for no override. Example:
            Europe/Paris.
        webhook_url (str | Unset): Webhook URL (webhook destination only) Example: https://example.com/webhook.
    """

    destination_type: SchedulesCreateSchedulesCreateBodyDestinationType
    format_: SchedulesCreateSchedulesCreateBodyFormat
    identifier: str
    name: str
    schedule: str
    timezone: str
    bucket_name: str | Unset = UNSET
    condition_query_map_key: str | Unset = UNSET
    condition_type: SchedulesCreateSchedulesCreateBodyConditionType | Unset = UNSET
    enable_formatting: bool | Unset = UNSET
    fan_out: bool | Unset = UNSET
    filter_config: Any | Unset = UNSET
    hide_hidden_fields: bool | Unset = UNSET
    hide_title: bool | Unset = UNSET
    key_prefix: str | Unset = UNSET
    kill_jobs_on_failure: bool | Unset = UNSET
    recipients: list[SchedulesCreateSchedulesCreateBodyRecipientsItem] | Unset = UNSET
    region: str | Unset = UNSET
    role_arn: str | Unset = UNSET
    show_content_link: bool | Unset = UNSET
    show_filters: bool | Unset = UNSET
    slack_recipient_type: str | Unset = UNSET
    test_now: bool | Unset = UNSET
    timezone_override: None | str | Unset = UNSET
    webhook_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        destination_type: str = self.destination_type

        format_: str = self.format_

        identifier = self.identifier

        name = self.name

        schedule = self.schedule

        timezone = self.timezone

        bucket_name = self.bucket_name

        condition_query_map_key = self.condition_query_map_key

        condition_type: str | Unset = UNSET
        if not isinstance(self.condition_type, Unset):
            condition_type = self.condition_type

        enable_formatting = self.enable_formatting

        fan_out = self.fan_out

        filter_config = self.filter_config

        hide_hidden_fields = self.hide_hidden_fields

        hide_title = self.hide_title

        key_prefix = self.key_prefix

        kill_jobs_on_failure = self.kill_jobs_on_failure

        recipients: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.recipients, Unset):
            recipients = []
            for recipients_item_data in self.recipients:
                recipients_item = recipients_item_data.to_dict()
                recipients.append(recipients_item)

        region = self.region

        role_arn = self.role_arn

        show_content_link = self.show_content_link

        show_filters = self.show_filters

        slack_recipient_type = self.slack_recipient_type

        test_now = self.test_now

        timezone_override: None | str | Unset
        if isinstance(self.timezone_override, Unset):
            timezone_override = UNSET
        else:
            timezone_override = self.timezone_override

        webhook_url = self.webhook_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "destinationType": destination_type,
                "format": format_,
                "identifier": identifier,
                "name": name,
                "schedule": schedule,
                "timezone": timezone,
            }
        )
        if bucket_name is not UNSET:
            field_dict["bucketName"] = bucket_name
        if condition_query_map_key is not UNSET:
            field_dict["conditionQueryMapKey"] = condition_query_map_key
        if condition_type is not UNSET:
            field_dict["conditionType"] = condition_type
        if enable_formatting is not UNSET:
            field_dict["enableFormatting"] = enable_formatting
        if fan_out is not UNSET:
            field_dict["fanOut"] = fan_out
        if filter_config is not UNSET:
            field_dict["filterConfig"] = filter_config
        if hide_hidden_fields is not UNSET:
            field_dict["hideHiddenFields"] = hide_hidden_fields
        if hide_title is not UNSET:
            field_dict["hideTitle"] = hide_title
        if key_prefix is not UNSET:
            field_dict["keyPrefix"] = key_prefix
        if kill_jobs_on_failure is not UNSET:
            field_dict["killJobsOnFailure"] = kill_jobs_on_failure
        if recipients is not UNSET:
            field_dict["recipients"] = recipients
        if region is not UNSET:
            field_dict["region"] = region
        if role_arn is not UNSET:
            field_dict["roleArn"] = role_arn
        if show_content_link is not UNSET:
            field_dict["showContentLink"] = show_content_link
        if show_filters is not UNSET:
            field_dict["showFilters"] = show_filters
        if slack_recipient_type is not UNSET:
            field_dict["slackRecipientType"] = slack_recipient_type
        if test_now is not UNSET:
            field_dict["testNow"] = test_now
        if timezone_override is not UNSET:
            field_dict["timezoneOverride"] = timezone_override
        if webhook_url is not UNSET:
            field_dict["webhookUrl"] = webhook_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.schedules_create_schedules_create_body_recipients_item import (
            SchedulesCreateSchedulesCreateBodyRecipientsItem,
        )

        d = dict(src_dict)
        destination_type = check_schedules_create_schedules_create_body_destination_type(d.pop("destinationType"))

        format_ = check_schedules_create_schedules_create_body_format(d.pop("format"))

        identifier = d.pop("identifier")

        name = d.pop("name")

        schedule = d.pop("schedule")

        timezone = d.pop("timezone")

        bucket_name = d.pop("bucketName", UNSET)

        condition_query_map_key = d.pop("conditionQueryMapKey", UNSET)

        _condition_type = d.pop("conditionType", UNSET)
        condition_type: SchedulesCreateSchedulesCreateBodyConditionType | Unset
        if isinstance(_condition_type, Unset):
            condition_type = UNSET
        else:
            condition_type = check_schedules_create_schedules_create_body_condition_type(_condition_type)

        enable_formatting = d.pop("enableFormatting", UNSET)

        fan_out = d.pop("fanOut", UNSET)

        filter_config = d.pop("filterConfig", UNSET)

        hide_hidden_fields = d.pop("hideHiddenFields", UNSET)

        hide_title = d.pop("hideTitle", UNSET)

        key_prefix = d.pop("keyPrefix", UNSET)

        kill_jobs_on_failure = d.pop("killJobsOnFailure", UNSET)

        _recipients = d.pop("recipients", UNSET)
        recipients: list[SchedulesCreateSchedulesCreateBodyRecipientsItem] | Unset = UNSET
        if _recipients is not UNSET:
            recipients = []
            for recipients_item_data in _recipients:
                recipients_item = SchedulesCreateSchedulesCreateBodyRecipientsItem.from_dict(recipients_item_data)

                recipients.append(recipients_item)

        region = d.pop("region", UNSET)

        role_arn = d.pop("roleArn", UNSET)

        show_content_link = d.pop("showContentLink", UNSET)

        show_filters = d.pop("showFilters", UNSET)

        slack_recipient_type = d.pop("slackRecipientType", UNSET)

        test_now = d.pop("testNow", UNSET)

        def _parse_timezone_override(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        timezone_override = _parse_timezone_override(d.pop("timezoneOverride", UNSET))

        webhook_url = d.pop("webhookUrl", UNSET)

        schedules_create_schedules_create_body = cls(
            destination_type=destination_type,
            format_=format_,
            identifier=identifier,
            name=name,
            schedule=schedule,
            timezone=timezone,
            bucket_name=bucket_name,
            condition_query_map_key=condition_query_map_key,
            condition_type=condition_type,
            enable_formatting=enable_formatting,
            fan_out=fan_out,
            filter_config=filter_config,
            hide_hidden_fields=hide_hidden_fields,
            hide_title=hide_title,
            key_prefix=key_prefix,
            kill_jobs_on_failure=kill_jobs_on_failure,
            recipients=recipients,
            region=region,
            role_arn=role_arn,
            show_content_link=show_content_link,
            show_filters=show_filters,
            slack_recipient_type=slack_recipient_type,
            test_now=test_now,
            timezone_override=timezone_override,
            webhook_url=webhook_url,
        )

        schedules_create_schedules_create_body.additional_properties = d
        return schedules_create_schedules_create_body

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

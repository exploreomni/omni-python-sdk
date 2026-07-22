from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agentic_job_attachment import AgenticJobAttachment
    from ..models.ai_job_submit_body_webhook_metadata import AiJobSubmitBodyWebhookMetadata


T = TypeVar("T", bound="AiJobSubmitBody")


@_attrs_define
class AiJobSubmitBody:
    """
    Attributes:
        model_id (UUID): The UUID of the model to query against. Must be a shared model, or a shared-extension model
            usable as a workbook base. Example: 770e8400-e29b-41d4-a716-446655440002.
        prompt (str): The natural language prompt for the AI to process. The AI will analyze your question, generate
            appropriate queries, execute them, and return a summarized answer. Example: What are the top 5 products by
            revenue this quarter?.
        attachments (list[AgenticJobAttachment] | Unset): Optional image or PDF attachments (e.g. a screenshot or export
            of a legacy BI dashboard being migrated) giving the AI additional visual context alongside the prompt. Up to 5
            files, sharing a combined 50000-token budget with the rest of the prompt.
        branch_id (UUID | Unset): Optional branch ID for the model. Must be a branch of the shared model specified by
            modelId. Queries run against the branch model, and if the AI makes model changes (organizations with agentic
            modeling enabled), they are written to this branch instead of a newly created one. If omitted and the AI makes
            model changes, a new branch is created automatically. Example: 550e8400-e29b-41d4-a716-446655440000.
        conversation_id (UUID | Unset): Conversation ID to continue an existing conversation thread. The AI will have
            access to the context from previous jobs in the same conversation. If omitted, a new conversation is created.
            Only one active job can exist per conversation. Example: 660e8400-e29b-41d4-a716-446655440001.
        progress_webhook_enabled (bool | Unset): When true, real-time progress events are POSTed to webhookUrl during
            execution (e.g., "Searching for revenue fields", "Query returned 42 rows"). Requires webhookUrl. Progress events
            are best-effort: single attempt, no retries, failures do not affect job execution. Default: False. Example:
            True.
        topic_name (str | Unset): Topic name to scope query generation. Topics define a set of related views and their
            join paths. If not provided, the AI will automatically select the best topic. Use the pick-topic endpoint to
            determine the right topic programmatically. Example: order_items.
        webhook_metadata (AiJobSubmitBodyWebhookMetadata | Unset): Arbitrary metadata object that will be included
            unchanged in webhook payloads. Use this to correlate webhook notifications with your own system (e.g., tracking
            IDs, channel references). Example: {'externalId': 'task-123', 'slackChannel': 'C0123456789'}.
        webhook_signing_secret (str | Unset): Secret key for HMAC-SHA256 webhook payload signing. When provided, each
            webhook request includes X-Omni-Signature and X-Omni-Signature-Timestamp headers for verification. Required if
            webhookUrl is specified.
        webhook_url (str | Unset): URL to receive webhook POSTs. Always receives a terminal event (job.complete,
            job.failed, or job.denied) when the job finishes; a job.denied event (e.g. the organization is over its AI
            credit limit) additionally carries a reason field. When progressWebhookEnabled is true, also receives real-time
            progress events during execution. Example: https://example.com/webhooks/omni.
    """

    model_id: UUID
    prompt: str
    attachments: list[AgenticJobAttachment] | Unset = UNSET
    branch_id: UUID | Unset = UNSET
    conversation_id: UUID | Unset = UNSET
    progress_webhook_enabled: bool | Unset = False
    topic_name: str | Unset = UNSET
    webhook_metadata: AiJobSubmitBodyWebhookMetadata | Unset = UNSET
    webhook_signing_secret: str | Unset = UNSET
    webhook_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_id = str(self.model_id)

        prompt = self.prompt

        attachments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.attachments, Unset):
            attachments = []
            for attachments_item_data in self.attachments:
                attachments_item = attachments_item_data.to_dict()
                attachments.append(attachments_item)

        branch_id: str | Unset = UNSET
        if not isinstance(self.branch_id, Unset):
            branch_id = str(self.branch_id)

        conversation_id: str | Unset = UNSET
        if not isinstance(self.conversation_id, Unset):
            conversation_id = str(self.conversation_id)

        progress_webhook_enabled = self.progress_webhook_enabled

        topic_name = self.topic_name

        webhook_metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.webhook_metadata, Unset):
            webhook_metadata = self.webhook_metadata.to_dict()

        webhook_signing_secret = self.webhook_signing_secret

        webhook_url = self.webhook_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "modelId": model_id,
                "prompt": prompt,
            }
        )
        if attachments is not UNSET:
            field_dict["attachments"] = attachments
        if branch_id is not UNSET:
            field_dict["branchId"] = branch_id
        if conversation_id is not UNSET:
            field_dict["conversationId"] = conversation_id
        if progress_webhook_enabled is not UNSET:
            field_dict["progressWebhookEnabled"] = progress_webhook_enabled
        if topic_name is not UNSET:
            field_dict["topicName"] = topic_name
        if webhook_metadata is not UNSET:
            field_dict["webhookMetadata"] = webhook_metadata
        if webhook_signing_secret is not UNSET:
            field_dict["webhookSigningSecret"] = webhook_signing_secret
        if webhook_url is not UNSET:
            field_dict["webhookUrl"] = webhook_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agentic_job_attachment import AgenticJobAttachment
        from ..models.ai_job_submit_body_webhook_metadata import AiJobSubmitBodyWebhookMetadata

        d = dict(src_dict)
        model_id = UUID(d.pop("modelId"))

        prompt = d.pop("prompt")

        _attachments = d.pop("attachments", UNSET)
        attachments: list[AgenticJobAttachment] | Unset = UNSET
        if _attachments is not UNSET:
            attachments = []
            for attachments_item_data in _attachments:
                attachments_item = AgenticJobAttachment.from_dict(attachments_item_data)

                attachments.append(attachments_item)

        _branch_id = d.pop("branchId", UNSET)
        branch_id: UUID | Unset
        if isinstance(_branch_id, Unset):
            branch_id = UNSET
        else:
            branch_id = UUID(_branch_id)

        _conversation_id = d.pop("conversationId", UNSET)
        conversation_id: UUID | Unset
        if isinstance(_conversation_id, Unset):
            conversation_id = UNSET
        else:
            conversation_id = UUID(_conversation_id)

        progress_webhook_enabled = d.pop("progressWebhookEnabled", UNSET)

        topic_name = d.pop("topicName", UNSET)

        _webhook_metadata = d.pop("webhookMetadata", UNSET)
        webhook_metadata: AiJobSubmitBodyWebhookMetadata | Unset
        if isinstance(_webhook_metadata, Unset):
            webhook_metadata = UNSET
        else:
            webhook_metadata = AiJobSubmitBodyWebhookMetadata.from_dict(_webhook_metadata)

        webhook_signing_secret = d.pop("webhookSigningSecret", UNSET)

        webhook_url = d.pop("webhookUrl", UNSET)

        ai_job_submit_body = cls(
            model_id=model_id,
            prompt=prompt,
            attachments=attachments,
            branch_id=branch_id,
            conversation_id=conversation_id,
            progress_webhook_enabled=progress_webhook_enabled,
            topic_name=topic_name,
            webhook_metadata=webhook_metadata,
            webhook_signing_secret=webhook_signing_secret,
            webhook_url=webhook_url,
        )

        ai_job_submit_body.additional_properties = d
        return ai_job_submit_body

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

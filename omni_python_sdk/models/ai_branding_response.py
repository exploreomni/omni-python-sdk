from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AiBrandingResponse")


@_attrs_define
class AiBrandingResponse:
    """
    Attributes:
        body (str): Body / description copy shown beneath the headline on AI helper landing surfaces. Example: I can
            help answer data questions, build a Dashboard, or create an App..
        headline (str): Short headline shown on AI helper landing surfaces. Example: What would you like to know?.
        logo_url (None | str): Absolute URL to a custom AI helper logo. `null` when the org has not configured a custom
            logo — clients should render their default avatar (e.g. Blobby). Example: https://example.com/blobby.png.
        name (str): Display name for the AI helper. Defaults to `Omni Agent` when no custom branding is set. Example:
            Blobby.
        placeholder (str): Placeholder text for the AI helper's prompt input. Example: Ask a question about your
            data....
    """

    body: str
    headline: str
    logo_url: None | str
    name: str
    placeholder: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        body = self.body

        headline = self.headline

        logo_url: None | str
        logo_url = self.logo_url

        name = self.name

        placeholder = self.placeholder

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "body": body,
                "headline": headline,
                "logoUrl": logo_url,
                "name": name,
                "placeholder": placeholder,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        body = d.pop("body")

        headline = d.pop("headline")

        def _parse_logo_url(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        logo_url = _parse_logo_url(d.pop("logoUrl"))

        name = d.pop("name")

        placeholder = d.pop("placeholder")

        ai_branding_response = cls(
            body=body,
            headline=headline,
            logo_url=logo_url,
            name=name,
            placeholder=placeholder,
        )

        ai_branding_response.additional_properties = d
        return ai_branding_response

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

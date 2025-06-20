from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, TypedDict

from typing_extensions import Required

if TYPE_CHECKING:
    from .common import ExternalDocs, ReferenceModel, Tag


class Contact(TypedDict, total=False):
    name: str
    url: str
    email: str


class License(TypedDict, total=False):
    name: Required[str]
    url: str


class Info(TypedDict, total=False):
    title: Required[str]
    version: Required[str]
    description: str
    termsOfService: str
    contact: Contact
    license: License
    tags: Sequence[ReferenceModel | Tag]
    externalDocs: ReferenceModel | ExternalDocs

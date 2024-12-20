from __future__ import annotations

from typing import Sequence, TypedDict

from typing_extensions import Required

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
    terms_of_service: str
    contact: Contact
    license: License
    tags: Sequence[ReferenceModel | Tag]
    external_docs: ReferenceModel | ExternalDocs

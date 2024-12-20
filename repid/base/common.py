from __future__ import annotations

from typing import TypedDict

from typing_extensions import Required

ReferenceObject = str

ReferenceModel = TypedDict(
    "ReferenceModel",
    {
        "$ref": ReferenceObject,
    },
)


class ExternalDocs(TypedDict, total=False):
    description: str
    url: Required[str]


class Tag(TypedDict, total=False):
    name: Required[str]
    description: str
    external_docs: ReferenceModel | ExternalDocs

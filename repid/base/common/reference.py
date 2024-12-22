from __future__ import annotations

from typing import TypedDict

ReferenceObject = str

ReferenceModel = TypedDict(
    "ReferenceModel",
    {
        "$ref": ReferenceObject,
    },
)

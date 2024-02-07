from __future__ import annotations

import importlib.metadata
import sys
from importlib.metadata import EntryPoint
from typing import Any, Iterator
from unittest.mock import patch

import pytest

from repid.config import Config


class SomeClass:
    def routing_key(self) -> str:
        return "routing_key"

    def parameters(self) -> str:
        return "parameters"

    def bucket(self) -> str:
        return "bucket"

    def result_bucket(self) -> str:
        return "result_bucket"

    def converter(self) -> str:
        return "converter"

    def serializer(self) -> str:
        return "serializer"


@pytest.fixture()
def _entry_points(monkeypatch: pytest.MonkeyPatch) -> None:
    def mock_entry_points() -> dict[str, list[EntryPoint]]:
        return {
            "repid_data": [
                EntryPoint("routing_key", "tests:test_config.SomeClass.routing_key", "repid_data"),
                EntryPoint("parameters", "tests:test_config.SomeClass.parameters", "repid_data"),
                EntryPoint("bucket", "tests:test_config.SomeClass.bucket", "repid_data"),
                EntryPoint(
                    "result_bucket",
                    "tests:test_config.SomeClass.result_bucket",
                    "repid_data",
                ),
            ],
            "repid_serializer": [
                EntryPoint(
                    "serializer",
                    "tests:test_config.SomeClass.serializer",
                    "repid_serializer",
                ),
            ],
            "repid_converter": [
                EntryPoint("converter", "tests:test_config.SomeClass.converter", "repid_converter"),
            ],
        }

    if sys.version_info >= (3, 10):

        def mock_entry_points(
            *args: Any,
            **kwargs: Any,
        ) -> importlib.metadata.EntryPoints:
            return importlib.metadata.EntryPoints(
                [
                    EntryPoint(
                        "routing_key",
                        "tests:test_config.SomeClass.routing_key",
                        "repid_data",
                    ),
                    EntryPoint(
                        "parameters",
                        "tests:test_config.SomeClass.parameters",
                        "repid_data",
                    ),
                    EntryPoint(
                        "bucket",
                        "tests:test_config.SomeClass.bucket",
                        "repid_data",
                    ),
                    EntryPoint(
                        "result_bucket",
                        "tests:test_config.SomeClass.result_bucket",
                        "repid_data",
                    ),
                    EntryPoint(
                        "serializer",
                        "tests:test_config.SomeClass.serializer",
                        "repid_serializer",
                    ),
                    EntryPoint(
                        "converter",
                        "tests:test_config.SomeClass.converter",
                        "repid_converter",
                    ),
                ],
            ).select(*args, **kwargs)

    monkeypatch.setattr(importlib.metadata, "entry_points", mock_entry_points)


@pytest.fixture()
def _patch_config() -> Iterator[None]:
    with patch.multiple(
        Config,
        ROUTING_KEY=Config.ROUTING_KEY,
        PARAMETERS=Config.PARAMETERS,
        BUCKET=Config.BUCKET,
        RESULT_BUCKET=Config.RESULT_BUCKET,
        SERIALIZER=Config.SERIALIZER,
        CONVERTER=Config.CONVERTER,
    ):
        yield


pytestmark = pytest.mark.usefixtures("_entry_points", "_patch_config")


def test_update_data_overrides() -> None:
    Config.update_data_overrides()

    assert Config.ROUTING_KEY is SomeClass.routing_key
    assert Config.PARAMETERS is SomeClass.parameters
    assert Config.BUCKET is SomeClass.bucket
    assert Config.RESULT_BUCKET is SomeClass.result_bucket


def test_update_serializer_override() -> None:
    Config.update_serializer_override()

    assert Config.SERIALIZER is SomeClass.serializer


def test_update_converter_override() -> None:
    Config.update_converter_override()

    assert Config.CONVERTER is SomeClass.converter


def test_update_all() -> None:
    Config.update_all()

    assert Config.ROUTING_KEY is SomeClass.routing_key
    assert Config.PARAMETERS is SomeClass.parameters
    assert Config.BUCKET is SomeClass.bucket
    assert Config.RESULT_BUCKET is SomeClass.result_bucket
    assert Config.SERIALIZER is SomeClass.serializer
    assert Config.CONVERTER is SomeClass.converter

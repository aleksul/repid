from __future__ import annotations

import importlib.metadata
from importlib.metadata import EntryPoint
from typing import Iterator
from unittest.mock import patch

import pytest
from pytest import MonkeyPatch

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


@pytest.fixture
def entry_points(monkeypatch: MonkeyPatch) -> None:
    def mock_entry_points() -> dict[str, list[EntryPoint]]:
        return {
            "repid_data": [
                EntryPoint("routing_key", "tests:test_config.SomeClass.routing_key", "repid_data"),
                EntryPoint("parameters", "tests:test_config.SomeClass.parameters", "repid_data"),
                EntryPoint("bucket", "tests:test_config.SomeClass.bucket", "repid_data"),
                EntryPoint(
                    "result_bucket", "tests:test_config.SomeClass.result_bucket", "repid_data"
                ),
            ],
            "repid_serializer": [
                EntryPoint(
                    "serializer", "tests:test_config.SomeClass.serializer", "repid_serializer"
                )
            ],
            "repid_converter": [
                EntryPoint("converter", "tests:test_config.SomeClass.converter", "repid_converter")
            ],
        }

    monkeypatch.setattr(importlib.metadata, "entry_points", mock_entry_points)


@pytest.fixture
def patch_config() -> Iterator[None]:
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


pytestmark = pytest.mark.usefixtures("entry_points", "patch_config")


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

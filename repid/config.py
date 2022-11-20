from __future__ import annotations

import importlib.metadata
from typing import TYPE_CHECKING

from repid.converter import DefaultConverter
from repid.data._buckets import ArgsBucket, ResultBucket
from repid.data._key import RoutingKey
from repid.data._parameters import Parameters
from repid.serializer import default_serializer

if TYPE_CHECKING:
    from repid.converter import ConverterT
    from repid.data.protocols import BucketT, ParametersT, ResultBucketT, RoutingKeyT
    from repid.serializer import SerializerT


class Config:
    ROUTING_KEY: type[RoutingKeyT] = RoutingKey
    PARAMETERS: type[ParametersT] = Parameters
    BUCKET: type[BucketT] = ArgsBucket
    RESULT_BUCKET: type[ResultBucketT] = ResultBucket
    SERIALIZER: SerializerT = default_serializer
    CONVERTER: type[ConverterT] = DefaultConverter

    @classmethod
    def update_data_overrides(cls) -> None:
        entry_points = importlib.metadata.entry_points().get("repid_data")
        if entry_points is None:
            return
        for entry_point in entry_points:
            if entry_point.name == "routing_key":
                cls.ROUTING_KEY = entry_point.load()
            elif entry_point.name == "parameters":
                cls.PARAMETERS = entry_point.load()
            elif entry_point.name == "bucket":
                cls.BUCKET = entry_point.load()
            elif entry_point.name == "result_bucket":
                cls.RESULT_BUCKET = entry_point.load()

    @classmethod
    def update_serializer_override(cls) -> None:
        entry_points = importlib.metadata.entry_points().get("repid_serializer")
        if entry_points is None:
            return
        for entry_point in entry_points:
            if entry_point.name == "serializer":
                cls.SERIALIZER = entry_point.load()

    @classmethod
    def update_converter_override(cls) -> None:
        entry_points = importlib.metadata.entry_points().get("repid_converter")
        if entry_points is None:
            return
        for entry_point in entry_points:
            if entry_point.name == "converter":
                cls.CONVERTER = entry_point.load()

    @classmethod
    def update_all(cls) -> None:
        cls.update_data_overrides()
        cls.update_serializer_override()
        cls.update_converter_override()

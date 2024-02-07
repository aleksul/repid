from __future__ import annotations

import importlib.metadata
import sys
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

if sys.version_info >= (3, 10):  # pragma: no cover
    _get_entrypoints = lambda x: importlib.metadata.entry_points(group=x)  # noqa: E731
else:
    _get_entrypoints = lambda x: importlib.metadata.entry_points().get(x)  # noqa: E731


class Config:
    """Configuration class that sets default values for various parameters."""

    ROUTING_KEY: type[RoutingKeyT] = RoutingKey
    PARAMETERS: type[ParametersT] = Parameters
    BUCKET: type[BucketT] = ArgsBucket
    RESULT_BUCKET: type[ResultBucketT] = ResultBucket
    SERIALIZER: SerializerT = default_serializer
    CONVERTER: type[ConverterT] = DefaultConverter

    @classmethod
    def update_data_overrides(cls) -> None:
        """Class method that updates default data structures.

        It gets the entry points from the "repid_data" metadata and loops through them.
        If it finds one of the keys (routing_key, parameters, bucket, result_bucket) it will update
        the appropriate class field.
        """
        entry_points = _get_entrypoints("repid_data")
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
        """Class method that updates default serializer.

        It gets the entry points from the "repid_serializer" metadata and loops through them.
        If it finds "serializer" key it will update the appropriate class field.
        """
        entry_points = _get_entrypoints("repid_serializer")
        if entry_points is None:
            return
        for entry_point in entry_points:
            if entry_point.name == "serializer":
                cls.SERIALIZER = entry_point.load()

    @classmethod
    def update_converter_override(cls) -> None:
        """Class method that updates default converter.

        It gets the entry points from the "repid_converter" metadata and loops through them.
        If it finds "converter" key it will update the appropriate class field.
        """
        entry_points = _get_entrypoints("repid_converter")
        if entry_points is None:
            return
        for entry_point in entry_points:
            if entry_point.name == "converter":
                cls.CONVERTER = entry_point.load()

    @classmethod
    def update_all(cls) -> None:
        """Class method that updates the data, serializer and converter overrides."""
        cls.update_data_overrides()
        cls.update_serializer_override()
        cls.update_converter_override()

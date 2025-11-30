from pydantic import BaseModel

from repid.serializer import default_serializer


def test_default_serializer_with_dict() -> None:
    data = {"key": "value"}
    out = default_serializer(data)
    assert isinstance(out, bytes)
    assert b'"key"' in out
    assert b'"value"' in out


def test_default_serializer_with_pydantic() -> None:
    class M(BaseModel):
        key: str

    m = M(key="value")
    out = default_serializer(m)
    assert isinstance(out, bytes)
    assert b'"key"' in out
    assert b'"value"' in out


def test_default_serializer_pydantic_inside_of_dict() -> None:
    class M(BaseModel):
        key: str

    data = {"model": M(key="value")}
    out = default_serializer(data)
    assert isinstance(out, bytes)
    assert b'"model"' in out
    assert b'"key"' in out
    assert b'"value"' in out

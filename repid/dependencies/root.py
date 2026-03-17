from __future__ import annotations


class Root:
    """Annotation marker designating a Pydantic model as the root payload.

    When applied as ``Annotated[MyModel, Root()]``, the model's fields are
    read directly from the top level of the incoming JSON rather than being
    nested under the argument name.  Only one ``Root()`` marker is allowed
    per actor.
    """

    __slots__ = ()

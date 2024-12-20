from __future__ import annotations

from typing import Literal, Mapping, Sequence, TypedDict

from typing_extensions import NotRequired, Required

from .common import ExternalDocs, ReferenceModel, Tag


class UserPassword(TypedDict, total=False):
    type: Required[Literal["userPassword"]]
    description: str


ApiKey = TypedDict(
    "ApiKey",
    {
        "type": Literal["apiKey"],
        "in": Literal["user", "password"],
        "description": NotRequired[str],
    },
)


class X509(TypedDict, total=False):
    type: Required[Literal["X509"]]
    description: NotRequired[str]


class SymmetricEncryption(TypedDict, total=False):
    type: Literal["symmetricEncryption"]
    description: NotRequired[str]


class AsymmetricEncryption(TypedDict, total=False):
    type: Literal["asymmetricEncryption"]
    description: NotRequired[str]


class NonBearerHTTPSecurityScheme(TypedDict, total=False):
    scheme: str
    description: NotRequired[str]
    type: Literal["http"]


class BearerHTTPSecurityScheme(TypedDict, total=False):
    scheme: Literal["bearer"]
    bearer_format: NotRequired[str]
    type: Literal["http"]
    description: NotRequired[str]


APIKeyHTTPSecurityScheme = TypedDict(
    "APIKeyHTTPSecurityScheme",
    {
        "type": Literal["httpApiKey"],
        "name": str,
        "in": Literal["header", "query", "cookie"],
        "description": NotRequired[str],
    },
)


Oauth2Scopes = Mapping[str, str] | None


class OpenIdConnect(TypedDict):
    type: Literal["openIdConnect"]
    description: NotRequired[str]
    open_id_connect_url: str
    scopes: NotRequired[Sequence[str]]


class SaslPlainSecurityScheme(TypedDict):
    type: Literal["plain"]
    description: NotRequired[str]


class SaslScramSecurityScheme(TypedDict):
    type: Literal["scramSha256", "scramSha512"]
    description: NotRequired[str]


class SaslGssapiSecurityScheme(TypedDict):
    type: Literal["gssapi"]
    description: NotRequired[str]


HTTPSecurityScheme = (
    NonBearerHTTPSecurityScheme | BearerHTTPSecurityScheme | APIKeyHTTPSecurityScheme
)


class Oauth2Flow(TypedDict):
    authorization_url: NotRequired[str]
    token_url: NotRequired[str]
    refresh_url: NotRequired[str]
    available_scopes: NotRequired[Oauth2Scopes]


SaslSecurityScheme = SaslPlainSecurityScheme | SaslScramSecurityScheme | SaslGssapiSecurityScheme


class Flows(TypedDict):
    implicit: NotRequired[Oauth2Flow]
    password: NotRequired[Oauth2Flow]
    client_credentials: NotRequired[Oauth2Flow]
    authorization_code: NotRequired[Oauth2Flow]


class Oauth2Flows(TypedDict):
    type: Literal["oauth2"]
    description: NotRequired[str]
    flows: Flows
    scopes: NotRequired[Sequence[str]]


SecurityScheme = (
    UserPassword
    | ApiKey
    | X509
    | SymmetricEncryption
    | AsymmetricEncryption
    | HTTPSecurityScheme
    | Oauth2Flows
    | OpenIdConnect
    | SaslSecurityScheme
)


class ServerVariable(TypedDict, total=False):
    enum: Sequence[str]
    default: str
    description: str
    examples: Sequence[str]


class Server(TypedDict, total=False):
    host: Required[str]
    pathname: str
    title: str
    summary: str
    description: str
    protocol: Required[str]
    protocol_version: str
    variables: Mapping[str, ReferenceModel | ServerVariable] | None
    security: Sequence[ReferenceModel | SecurityScheme]
    tags: Sequence[ReferenceModel | Tag]
    external_docs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | ServerBindingsObject

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, TypedDict

from typing_extensions import NotRequired, Required


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
    description: str


class SymmetricEncryption(TypedDict, total=False):
    type: Required[Literal["symmetricEncryption"]]
    description: str


class AsymmetricEncryption(TypedDict, total=False):
    type: Required[Literal["asymmetricEncryption"]]
    description: str


class NonBearerHTTPSecurityScheme(TypedDict, total=False):
    type: Required[Literal["http"]]
    scheme: Required[str]
    description: str


class BearerHTTPSecurityScheme(TypedDict, total=False):
    type: Required[Literal["http"]]
    scheme: Required[Literal["bearer"]]
    description: str
    bearer_format: str


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


class OpenIdConnect(TypedDict, total=False):
    type: Required[Literal["openIdConnect"]]
    description: str
    open_id_connect_url: Required[str]
    scopes: Sequence[str]


class SaslPlainSecurityScheme(TypedDict, total=False):
    type: Required[Literal["plain"]]
    description: str


class SaslScramSecurityScheme(TypedDict, total=False):
    type: Required[Literal["scramSha256", "scramSha512"]]
    description: str


class SaslGssapiSecurityScheme(TypedDict, total=False):
    type: Required[Literal["gssapi"]]
    description: str


HTTPSecurityScheme = (
    NonBearerHTTPSecurityScheme | BearerHTTPSecurityScheme | APIKeyHTTPSecurityScheme
)


class Oauth2Flow(TypedDict, total=False):
    authorization_url: str
    token_url: str
    refresh_url: str
    available_scopes: Oauth2Scopes


SaslSecurityScheme = SaslPlainSecurityScheme | SaslScramSecurityScheme | SaslGssapiSecurityScheme


class Flows(TypedDict, total=False):
    implicit: Oauth2Flow
    password: Oauth2Flow
    client_credentials: Oauth2Flow
    authorization_code: Oauth2Flow


class Oauth2Flows(TypedDict, total=False):
    type: Required[Literal["oauth2"]]
    description: str
    flows: Required[Flows]
    scopes: Sequence[str]


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

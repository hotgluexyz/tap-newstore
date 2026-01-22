"""NewStore Authentication."""

from __future__ import annotations

import sys

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

class NewStoreAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for NewStore."""

    @override
    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

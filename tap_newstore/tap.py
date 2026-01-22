"""NewStore tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_newstore import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapNewStore(Tap):
    """Singer tap for NewStore."""

    name = "tap-newstore"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
        ),
        th.Property(
            "tenant",
            th.StringType(nullable=False),
            required=True,
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.NewStoreStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.StoresStream(self),
            streams.ShopsStream(self),
            streams.ProductsStream(self),
            streams.AvailabilitiesStream(self),
        ]


if __name__ == "__main__":
    TapNewStore.cli()

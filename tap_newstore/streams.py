"""Stream type classes for tap-newstore."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BaseAPIPaginator

from tap_newstore.client import NewStoreStream

import requests
from collections.abc import Iterable
from typing import Any
from singer_sdk.helpers.jsonpath import extract_jsonpath
from typing_extensions import override
import decimal

class StoresStream(NewStoreStream):
    name = "stores"
    path = "/v0/d/stores"
    primary_keys = ("store_id",)
    replication_key = None
    records_jsonpath = "$.stores[*]"

    schema = th.PropertiesList(
        th.Property("store_id", th.StringType),
        th.Property("label", th.StringType),
        th.Property("locale", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict) -> dict:
        return {
            "store_id": record["store_id"]
        }


class ShopsStream(NewStoreStream):
    name = "shops"
    path = "/v0/c/shops"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.shops[*]"
    parent_stream_type = StoresStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("locale", th.StringType),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        records = extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

        for record in records:
            for locale in record["locales"]:
                yield {
                    "id": record["id"],
                    "locale": locale["locale"],
                }

    def get_child_context(self, record: dict, context: dict) -> dict:
        return {
            "shop_id": record["id"],
            "locale": record["locale"],
            "store_id": context["store_id"],
        }


class OffsetPaginator(BaseAPIPaginator):
    """Offset-based paginator for ProductsStream."""

    def __init__(self, start_value: int = 0, page_size: int = 500) -> None:
        """Initialize the paginator.

        Args:
            start_value: The starting offset value.
            page_size: The number of records per page (count parameter).
        """
        super().__init__(start_value)
        self.page_size = page_size
        self.total: int | None = None

    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page token from the response.

        Args:
            response: The HTTP response object.

        Returns:
            The next offset value, or None if there are no more pages.
        """
        data = response.json()
        pagination = data.get("pagination", {})

        # Extract pagination info
        current_offset = pagination.get("offset", 0)
        self.total = pagination.get("total", 0)
        count = pagination.get("count", self.page_size)

        # Calculate next offset
        next_offset = current_offset + count

        # Return None if we've reached the end
        if next_offset >= self.total:
            return None

        return next_offset


class ProductsStream(NewStoreStream):
    name = "products"
    path = "/api/v1/shops/storefront-catalog-en/products?locale=en-us"
    primary_keys = ("product_id",)
    replication_key = None
    records_jsonpath = "$.elements[*]"
    # parent_stream_type = ShopsStream

    schema = th.PropertiesList(
        th.Property("product_id", th.StringType),
        th.Property("title", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new offset-based paginator instance.

        Returns:
            An OffsetPaginator instance with page size of 500.
        """
        return OffsetPaginator(start_value=0, page_size=500)

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return URL parameters including count and offset for pagination.

        Args:
            context: The stream context.
            next_page_token: The next offset value from paginator.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {
            "count": 500,  # Maximum page size
        }

        if next_page_token is not None:
            params["offset"] = next_page_token
        else:
            params["offset"] = 0

        return params

    def get_child_context(self, record: dict, context: dict) -> dict:
        return {
            # "shop_id": context["shop_id"],
            # "locale": context["locale"],
            "product_id": record["product_id"],
            # "store_id": context["store_id"],
        }


class AvailabilitiesStream(NewStoreStream):
    name = "availabilities"
    path = "/v0/availabilities"
    primary_keys = ("product_id",)
    replication_key = None
    records_jsonpath = "$.items[*]"
    parent_stream_type = ProductsStream
    http_method = "POST"

    schema = th.PropertiesList(
        th.Property("product_id", th.StringType),
        th.Property("fulfillment_node_id", th.StringType),
        th.Property("atp", th.IntegerType),
    ).to_dict()

    @override
    def prepare_request_payload(
        self,
        context,
        next_page_token,
    ) -> dict | None:
        return {
            "atp_keys": [
                {
                    "product_id": context["product_id"],
                    "fulfillment_node_id": "store1_NYC"
                }
            ]
        }
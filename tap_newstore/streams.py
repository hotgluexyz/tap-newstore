"""Stream type classes for tap-newstore."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_newstore.client import NewStoreStream

import requests
from collections.abc import Iterable
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


class ProductsStream(NewStoreStream):
    name = "products"
    path = "/api/v1/shops/{shop_id}/products?locale={locale}"
    primary_keys = ("product_id",)
    replication_key = None
    records_jsonpath = "$.elements[*]"
    parent_stream_type = ShopsStream

    schema = th.PropertiesList(
        th.Property("product_id", th.StringType),
        th.Property("title", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict) -> dict:
        return {
            "shop_id": context["shop_id"],
            "locale": context["locale"],
            "product_id": record["product_id"],
            "store_id": context["store_id"],
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
                    "fulfillment_node_id": context["store_id"],
                }
            ]
        }
import dlt
import json
import sys
import typing as t

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources

@dlt.source(
    name="northwind",
    parallelized=True
)
def source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/northwind/odata/v1/"
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0,
            "endpoint": {
                "data_selector": "value",
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "['@odata.nextLink']",
                },
                "params": {
                    "$count": "false",
                },
            },
        },
        "resources": [
            {
                "name": "categories",
                "primary_key": "CategoryId",
                "endpoint": {
                    "path": "Categories",
                }
            },

            {
                "name": "customers",
                "primary_key": "CustomerId",
                "endpoint": {
                    "path": "Customers",
                }
            },

            {
                "name": "employees",
                "primary_key": "EmployeeId",
                "endpoint": {
                    "path": "Employees",
                }
            },

            {
                "name": "employee_territories",
                "primary_key": ["EmployeeId", "TerritoryId"],
                "endpoint": {
                    "path": "Employees({id})/EmployeeTerritories",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "employees",
                            "field": "EmployeeId"
                        }
                    },
                }
            },

            {
                "name": "order_details",
                "primary_key": ["OrderId", "ProductId"],
                "endpoint": {
                    "path": "OrderDetails",
                }
            },

            {
                "name": "orders",
                "primary_key": "OrderId",
                "endpoint": {
                    "path": "Orders",
                }
            },

            {
                "name": "products",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "Products",
                }
            },

            {
                "name": "regions",
                "primary_key": "RegionId",
                "endpoint": {
                    "path": "Regions",
                }
            },

            {
                "name": "shippers",
                "primary_key": "ShipperId",
                "endpoint": {
                    "path": "Shippers",
                }
            },

            {
                "name": "suppliers",
                "primary_key": "SupplierId",
                "endpoint": {
                    "path": "Suppliers",
                }
            },

            {
                "name": "territories",
                "primary_key": "TerritoryId",
                "endpoint": {
                    "path": "Territories",
                }
            },

        ]
    }

    yield from rest_api_resources(source_config)

pipeline = dlt.pipeline(
    pipeline_name="northwind_pipeline",
    destination=dlt.destinations.duckdb("./adss.duckdb"),
    dataset_name="das",
    progress="alive_progress"
)

pipeline.run(source())
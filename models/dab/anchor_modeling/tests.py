"""
Tests for anchor modeling blueprint and sync modules.

Run with: pytest models/dab/anchor_modeling/tests.py -v
"""

import tempfile
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from . import blueprint
from . import sync


# ---------------------------------------------------------------------------
# Blueprint Tests - Keyset Expression
# ---------------------------------------------------------------------------


class TestBuildKeysetExpression:
    def test_single_key(self):
        expr = blueprint._build_keyset_expression("Product", "nw", "product_id")
        sql = expr.sql()
        assert "Product@nw|" in sql
        assert "product_id" in sql

    def test_single_key_with_tenant(self):
        expr = blueprint._build_keyset_expression("Product", "nw", "product_id", tenant="acme")
        sql = expr.sql()
        assert "Product@nw~acme|" in sql

    def test_composite_key(self):
        expr = blueprint._build_keyset_expression("OrderDetail", "nw", ["order_id", "product_id"])
        sql = expr.sql()
        assert "OrderDetail@nw|" in sql
        assert "order_id" in sql
        assert "product_id" in sql


# ---------------------------------------------------------------------------
# Blueprint Tests - Union All
# ---------------------------------------------------------------------------


class TestUnionAll:
    def test_single_select(self):
        select = exp.select("a", "b").from_("table1")
        result = blueprint._union_all([select])
        assert result is select

    def test_multiple_selects(self):
        select1 = exp.select("a", "b").from_("table1")
        select2 = exp.select("a", "b").from_("table2")
        result = blueprint._union_all([select1, select2])
        sql = result.sql()
        assert "UNION ALL" in sql
        assert "table1" in sql
        assert "table2" in sql

    def test_three_selects(self):
        selects = [exp.select("x").from_(f"t{i}") for i in range(3)]
        result = blueprint._union_all(selects)
        sql = result.sql()
        assert sql.count("UNION ALL") == 2


# ---------------------------------------------------------------------------
# Blueprint Tests - Tie Unique Keys
# ---------------------------------------------------------------------------


class TestBuildTieUniqueKeys:
    def test_distinct_anchor_types(self):
        roles = [
            {"type": "OR", "role": "order"},
            {"type": "PR", "role": "product"},
        ]
        keys = blueprint._build_tie_unique_keys(roles)
        assert keys == ["OR_ID_order", "PR_ID_product"]

    def test_same_anchor_type_different_roles(self):
        roles = [
            {"type": "PE", "role": "manager"},
            {"type": "PE", "role": "employee"},
        ]
        keys = blueprint._build_tie_unique_keys(roles)
        assert keys == ["PE_ID_manager", "PE_ID_employee"]

    def test_mixed_anchor_types(self):
        roles = [
            {"type": "OR", "role": "order"},
            {"type": "PE", "role": "manager"},
            {"type": "PE", "role": "employee"},
        ]
        keys = blueprint._build_tie_unique_keys(roles)
        assert keys == ["OR_ID_order", "PE_ID_manager", "PE_ID_employee"]


# ---------------------------------------------------------------------------
# Blueprint Tests - Validation
# ---------------------------------------------------------------------------


class TestValidateAnchorSources:
    def test_valid_anchor(self):
        model_data = {
            "anchors": {
                "PR": {
                    "descriptor": "Product",
                    "sources": [{"system": "nw", "table": "products", "key": "product_id"}]
                }
            }
        }
        stubs = blueprint._validate_anchor_sources(model_data)
        assert stubs == []

    def test_missing_sources_returns_stub(self):
        model_data = {
            "anchors": {
                "PR": {"descriptor": "Product", "sources": []}
            }
        }
        stubs = blueprint._validate_anchor_sources(model_data)
        assert len(stubs) == 1
        assert "PR:" in stubs[0]
        assert "Product" in stubs[0]
        assert "system: ???" in stubs[0]

    def test_missing_required_field_returns_stub(self):
        model_data = {
            "anchors": {
                "PR": {
                    "descriptor": "Product",
                    "sources": [{"system": "nw", "table": "products"}]  # missing key
                }
            }
        }
        stubs = blueprint._validate_anchor_sources(model_data)
        assert len(stubs) == 1
        assert "PR:" in stubs[0]
        assert "['key']" in stubs[0]

    def test_multiple_missing_anchors_collected(self):
        model_data = {
            "anchors": {
                "PR": {"descriptor": "Product", "sources": []},
                "OR": {"descriptor": "Order", "sources": []},
            }
        }
        stubs = blueprint._validate_anchor_sources(model_data)
        assert len(stubs) == 2


class TestValidateTieSources:
    def test_valid_tie(self):
        model_data = {
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": [{"system": "nw", "table": "order_details", "keys": {"OR": "order_id", "PR": "product_id"}}]
                }
            }
        }
        stubs = blueprint._validate_tie_sources(model_data)
        assert stubs == []

    def test_missing_sources_returns_stub(self):
        model_data = {
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": []
                }
            }
        }
        stubs = blueprint._validate_tie_sources(model_data)
        assert len(stubs) == 1
        assert "OR_order_PR_product:" in stubs[0]
        assert "OR: ???" in stubs[0]
        assert "PR: ???" in stubs[0]

    def test_missing_keys_field_returns_stub(self):
        model_data = {
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": [{"system": "nw", "table": "order_details"}]  # missing keys
                }
            }
        }
        stubs = blueprint._validate_tie_sources(model_data)
        assert len(stubs) == 1
        assert "missing fields" in stubs[0]
        assert "keys" in stubs[0]

    def test_self_referencing_tie_stub_has_role_specific_keys(self):
        """Test that ties with same anchor type use role-specific keys in stub."""
        model_data = {
            "ties": {
                "PE_manager_PE_employee": {
                    "roles": [
                        {"type": "PE", "role": "manager"},
                        {"type": "PE", "role": "employee"},
                    ],
                    "sources": []
                }
            }
        }
        stubs = blueprint._validate_tie_sources(model_data)
        assert len(stubs) == 1
        assert "PE_manager: ???" in stubs[0]
        assert "PE_employee: ???" in stubs[0]

    def test_multiple_missing_ties_collected(self):
        model_data = {
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": []
                },
                "PR_supplier_SU_by": {
                    "roles": [{"type": "PR", "role": "supplier"}, {"type": "SU", "role": "by"}],
                    "sources": []
                }
            }
        }
        stubs = blueprint._validate_tie_sources(model_data)
        assert len(stubs) == 2


class TestValidateModel:
    def test_valid_model_passes(self):
        model_data = {
            "anchors": {
                "PR": {
                    "descriptor": "Product",
                    "sources": [{"system": "nw", "table": "products", "key": "product_id"}]
                }
            },
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": [{"system": "nw", "table": "order_details", "keys": {"OR": "order_id", "PR": "product_id"}}]
                }
            }
        }
        blueprint._validate_model(model_data)  # Should not raise

    def test_multiple_errors_collected_in_single_exception(self):
        """Test that all validation errors are collected and raised together."""
        model_data = {
            "anchors": {
                "PR": {"descriptor": "Product", "sources": []},
                "OR": {"descriptor": "Order", "sources": []},
            },
            "ties": {
                "OR_order_PR_product": {
                    "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
                    "sources": []
                }
            }
        }
        with pytest.raises(blueprint.ModelValidationError) as exc_info:
            blueprint._validate_model(model_data)

        error_msg = str(exc_info.value)
        # Check header
        assert "Missing or incomplete source mappings" in error_msg
        assert "Add these entries to sources.yaml" in error_msg

        # Check that both anchors are included
        assert "PR:" in error_msg
        assert "OR:" in error_msg

        # Check that tie is included
        assert "OR_order_PR_product:" in error_msg

        # Check YAML structure
        assert "anchors:" in error_msg
        assert "ties:" in error_msg
        assert "system: ???" in error_msg
        assert "keys:" in error_msg

    def test_error_message_is_valid_yaml_structure(self):
        """Test that the error message contains properly formatted YAML stubs."""
        model_data = {
            "anchors": {
                "PR": {"descriptor": "Product", "sources": []}
            },
            "ties": {}
        }
        with pytest.raises(blueprint.ModelValidationError) as exc_info:
            blueprint._validate_model(model_data)

        error_msg = str(exc_info.value)
        # Should have proper indentation
        assert "  PR:" in error_msg
        assert "    - system: ???" in error_msg
        assert "      table: ???" in error_msg
        assert "      key: ???" in error_msg

    def test_self_referencing_tie_error_shows_role_specific_keys(self):
        """Test that error stubs for self-referencing ties show role-specific keys."""
        model_data = {
            "anchors": {},
            "ties": {
                "PE_manager_PE_employee": {
                    "roles": [
                        {"type": "PE", "role": "manager"},
                        {"type": "PE", "role": "employee"},
                    ],
                    "sources": []
                }
            }
        }
        with pytest.raises(blueprint.ModelValidationError) as exc_info:
            blueprint._validate_model(model_data)

        error_msg = str(exc_info.value)
        assert "PE_manager: ???" in error_msg
        assert "PE_employee: ???" in error_msg


# ---------------------------------------------------------------------------
# Blueprint Tests - Anchor Query Generation
# ---------------------------------------------------------------------------


class TestBuildAnchorSelect:
    def test_basic_select(self):
        source = {"system": "nw", "table": "products", "key": "product_id"}
        select = blueprint._build_anchor_select("PR", "Product", source, "2024-01-01T00:00:00")
        sql = select.sql()
        assert "PR_ID" in sql
        assert "PR_System" in sql
        assert "PR_Tenant" in sql
        assert "PR_ChangedAt" in sql
        assert "PR_LoadedAt" in sql
        assert "products" in sql

    def test_select_with_tenant(self):
        source = {"system": "nw", "table": "products", "key": "product_id", "tenant": "acme"}
        select = blueprint._build_anchor_select("PR", "Product", source, "2024-01-01T00:00:00")
        sql = select.sql()
        assert "acme" in sql


# ---------------------------------------------------------------------------
# Blueprint Tests - Tie Query Generation
# ---------------------------------------------------------------------------


class TestBuildTieSelect:
    def test_basic_tie_select(self):
        roles = [
            {"type": "OR", "role": "order"},
            {"type": "PR", "role": "product"},
        ]
        source = {
            "system": "nw",
            "table": "order_details",
            "keys": {"OR": "order_id", "PR": "product_id"},
        }
        anchor_descriptors = {"OR": "Order", "PR": "Product"}
        select = blueprint._build_tie_select(
            "OR_order_PR_product", roles, source, anchor_descriptors, "2024-01-01T00:00:00"
        )
        sql = select.sql()
        assert "OR_ID_order" in sql
        assert "PR_ID_product" in sql
        assert "OR_order_PR_product_System" in sql
        assert "OR_order_PR_product_LoadedAt" in sql
        assert "order_details" in sql

    def test_tie_select_with_role_specific_keys(self):
        """Test tie with same anchor type appearing twice (e.g., manager/employee)."""
        roles = [
            {"type": "PE", "role": "manager"},
            {"type": "PE", "role": "employee"},
        ]
        source = {
            "system": "nw",
            "table": "reports_to",
            "keys": {"PE_manager": "manager_id", "PE_employee": "employee_id"},
        }
        anchor_descriptors = {"PE": "Person"}
        select = blueprint._build_tie_select(
            "PE_manager_PE_employee", roles, source, anchor_descriptors, "2024-01-01T00:00:00"
        )
        sql = select.sql()
        assert "PE_ID_manager" in sql
        assert "PE_ID_employee" in sql

    def test_tie_select_missing_key_raises(self):
        roles = [{"type": "XX", "role": "unknown"}]
        source = {"system": "nw", "table": "t", "keys": {}}
        with pytest.raises(ValueError, match="no key mapping"):
            blueprint._build_tie_select("XX_unknown", roles, source, {"XX": "X"}, "2024-01-01")


# ---------------------------------------------------------------------------
# Blueprint Tests - Incremental Query
# ---------------------------------------------------------------------------


class TestBuildIncrementalQuery:
    def test_generates_anti_join_pattern(self):
        source_query = exp.select("id", "name").from_("source_table")
        query = blueprint._build_incremental_query(
            source_query=source_query,
            model_name="test_model",
            unique_keys=["id"],
            loaded_at_col="loaded_at",
            output_columns=[("id", "VARCHAR"), ("name", "VARCHAR")],
        )
        sql = query.sql()
        assert "WITH" in sql
        assert "target" in sql
        assert "source" in sql
        assert "ANTI" in sql or "LEFT" in sql  # anti join syntax varies
        assert "ROW_NUMBER()" in sql
        assert "dab.test_model" in sql  # Uses explicit table reference

    def test_multiple_unique_keys(self):
        source_query = exp.select("a", "b", "val").from_("t")
        query = blueprint._build_incremental_query(
            source_query=source_query,
            model_name="model",
            unique_keys=["a", "b"],
            loaded_at_col="loaded_at",
            output_columns=[("a", "VARCHAR"), ("b", "VARCHAR"), ("val", "VARCHAR")],
        )
        sql = query.sql()
        assert "a" in sql
        assert "b" in sql


# ---------------------------------------------------------------------------
# Blueprint Tests - Full Query Generation
# ---------------------------------------------------------------------------


class TestBuildAnchorQuery:
    def test_builds_full_anchor_query(self):
        bp = {
            "mnemonic": "PR",
            "descriptor": "Product",
            "sources": [{"system": "nw", "table": "products", "key": "product_id"}],
        }
        query = blueprint._build_anchor_query(bp, "2024-01-01T00:00:00", "dab.anchor__PR")
        sql = query.sql()
        assert "PR_ID" in sql
        assert "PR_System" in sql
        assert "PR_LoadedAt" in sql
        assert "WITH" in sql

    def test_anchor_query_no_sources_raises(self):
        bp = {"mnemonic": "PR", "descriptor": "Product", "sources": []}
        with pytest.raises(ValueError, match="No sources defined"):
            blueprint._build_anchor_query(bp, "2024-01-01", "model")


class TestBuildTieQuery:
    def test_builds_full_tie_query(self):
        bp = {
            "name": "OR_order_PR_product",
            "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
            "sources": [{"system": "nw", "table": "order_details", "keys": {"OR": "order_id", "PR": "product_id"}}],
            "anchor_descriptors": {"OR": "Order", "PR": "Product"},
        }
        query = blueprint._build_tie_query(bp, "2024-01-01T00:00:00", "dab.tie__test")
        sql = query.sql()
        assert "OR_ID_order" in sql
        assert "PR_ID_product" in sql
        assert "OR_order_PR_product_System" in sql
        assert "OR_order_PR_product_LoadedAt" in sql
        assert "WITH" in sql

    def test_tie_query_no_sources_raises(self):
        bp = {"name": "test", "roles": [], "sources": [], "anchor_descriptors": {}}
        with pytest.raises(ValueError, match="No sources defined"):
            blueprint._build_tie_query(bp, "2024-01-01", "model")


class TestBuildQuery:
    def test_dispatches_to_anchor(self):
        bp = {
            "entity_type": "anchor",
            "mnemonic": "PR",
            "descriptor": "Product",
            "sources": [{"system": "nw", "table": "products", "key": "product_id"}],
        }
        query = blueprint._build_query(bp, "2024-01-01", "dab.anchor__PR")
        assert query is not None

    def test_dispatches_to_tie(self):
        bp = {
            "entity_type": "tie",
            "name": "test",
            "roles": [{"type": "OR", "role": "order"}, {"type": "PR", "role": "product"}],
            "sources": [{"system": "nw", "table": "t", "keys": {"OR": "a", "PR": "b"}}],
            "anchor_descriptors": {"OR": "Order", "PR": "Product"},
        }
        query = blueprint._build_query(bp, "2024-01-01", "dab.tie__test")
        assert query is not None

    def test_unknown_entity_type_raises(self):
        bp = {"entity_type": "unknown"}
        with pytest.raises(ValueError, match="Unknown entity type"):
            blueprint._build_query(bp, "2024-01-01", "model")


# ---------------------------------------------------------------------------
# Sync Tests - Case Conversion
# ---------------------------------------------------------------------------


class TestToSnakeCase:
    def test_pascal_case(self):
        assert sync.to_snake_case("ProductId") == "product_id"

    def test_camel_case(self):
        assert sync.to_snake_case("productId") == "product_id"

    def test_already_snake_case(self):
        assert sync.to_snake_case("product_id") == "product_id"

    def test_all_caps_word(self):
        assert sync.to_snake_case("HTTPResponse") == "http_response"

    def test_single_word(self):
        assert sync.to_snake_case("Product") == "product"


# ---------------------------------------------------------------------------
# Sync Tests - Value Parsing
# ---------------------------------------------------------------------------


class TestParseValue:
    def test_true_string(self):
        assert sync.parse_value("true") is True
        assert sync.parse_value("True") is True
        assert sync.parse_value("TRUE") is True

    def test_false_string(self):
        assert sync.parse_value("false") is False
        assert sync.parse_value("False") is False

    def test_integer(self):
        assert sync.parse_value("42") == 42
        assert sync.parse_value("-10") == -10

    def test_float(self):
        assert sync.parse_value("3.14") == 3.14
        assert sync.parse_value("-2.5") == -2.5

    def test_string(self):
        assert sync.parse_value("hello") == "hello"
        assert sync.parse_value("Product") == "Product"


# ---------------------------------------------------------------------------
# Sync Tests - Tie Naming
# ---------------------------------------------------------------------------


class TestBuildTieName:
    def test_two_anchors(self):
        roles = [
            {"type": "OR", "role": "order", "identifier": True},
            {"type": "PR", "role": "product", "identifier": False},
        ]
        name = sync.build_tie_name(roles)
        assert name == "OR_order_PR_product"

    def test_identifier_first(self):
        roles = [
            {"type": "PR", "role": "product", "identifier": False},
            {"type": "OR", "role": "order", "identifier": True},
        ]
        name = sync.build_tie_name(roles)
        # Identifier should come first
        assert name.startswith("OR_order")

    def test_alphabetical_when_same_identifier(self):
        roles = [
            {"type": "ZZ", "role": "zebra", "identifier": False},
            {"type": "AA", "role": "alpha", "identifier": False},
        ]
        name = sync.build_tie_name(roles)
        assert name == "AA_alpha_ZZ_zebra"


# ---------------------------------------------------------------------------
# Sync Tests - XML Parsing
# ---------------------------------------------------------------------------


class TestParseDescriptionSources:
    def test_no_description(self):
        import xml.etree.ElementTree as ET
        elem = ET.fromstring("<anchor mnemonic='PR'/>")
        sources = sync.parse_description_sources(elem)
        assert sources == []

    def test_single_source(self):
        import xml.etree.ElementTree as ET
        xml = """
        <anchor mnemonic="PR">
            <description>
                <source system="nw" table="products">
                    <key>ProductId</key>
                </source>
            </description>
        </anchor>
        """
        elem = ET.fromstring(xml)
        sources = sync.parse_description_sources(elem)
        assert len(sources) == 1
        assert sources[0]["system"] == "nw"
        assert sources[0]["table"] == "products"
        assert sources[0]["key"] == "product_id"  # snake_case converted

    def test_composite_key(self):
        import xml.etree.ElementTree as ET
        xml = """
        <anchor mnemonic="OD">
            <description>
                <source system="nw" table="order_details">
                    <key>
                        <col>OrderId</col>
                        <col>ProductId</col>
                    </key>
                </source>
            </description>
        </anchor>
        """
        elem = ET.fromstring(xml)
        sources = sync.parse_description_sources(elem)
        assert len(sources) == 1
        assert sources[0]["key"] == ["order_id", "product_id"]

    def test_tie_keys_mapping(self):
        import xml.etree.ElementTree as ET
        xml = """
        <anchorRole type="OR" role="order">
            <description>
                <source system="nw" table="order_details">
                    <keys>
                        <OR>OrderId</OR>
                        <PR>ProductId</PR>
                    </keys>
                </source>
            </description>
        </anchorRole>
        """
        elem = ET.fromstring(xml)
        sources = sync.parse_description_sources(elem)
        assert len(sources) == 1
        assert sources[0]["keys"] == {"OR": "order_id", "PR": "product_id"}


# ---------------------------------------------------------------------------
# Sync Tests - Round Trip
# ---------------------------------------------------------------------------


class TestSyncRoundTrip:
    def test_xml_to_yaml_and_back(self):
        import xml.etree.ElementTree as ET

        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<schema format="0.99">
    <anchor mnemonic="PR" descriptor="Product">
        <description>
            <source system="nw" table="products">
                <key>product_id</key>
            </source>
        </description>
    </anchor>
</schema>"""

        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = Path(tmpdir) / "model.xml"
            yaml_path = Path(tmpdir) / "model.yaml"

            xml_path.write_text(xml_content)

            result = sync.sync_model(xml_path, yaml_path)

            assert "PR" in result["model"]["anchors"]
            assert yaml_path.exists()

            # Verify YAML was written
            import yaml
            with open(yaml_path) as f:
                yaml_data = yaml.safe_load(f)
            assert "PR" in yaml_data["anchors"]

            # Verify XML still has sources
            tree = ET.parse(xml_path)
            root = tree.getroot()
            anchor = root.find("anchor")
            desc = anchor.find("description")
            assert desc is not None
            source = desc.find("source")
            assert source is not None
            assert source.get("system") == "nw"


# ---------------------------------------------------------------------------
# Integration Test - Full Blueprint
# ---------------------------------------------------------------------------


class TestGetBlueprints:
    def test_loads_from_model_yaml(self):
        blueprints = blueprint._get_blueprints()
        assert len(blueprints) > 0

        anchor_blueprints = [b for b in blueprints if b["entity_type"] == "anchor"]
        tie_blueprints = [b for b in blueprints if b["entity_type"] == "tie"]

        assert len(anchor_blueprints) > 0
        assert len(tie_blueprints) > 0

        for bp in anchor_blueprints:
            assert "model_name" in bp
            assert "mnemonic" in bp
            assert "descriptor" in bp
            assert bp["model_name"].startswith("anchor__")

        for bp in tie_blueprints:
            assert "model_name" in bp
            assert "roles" in bp
            assert bp["model_name"].startswith("tie__")

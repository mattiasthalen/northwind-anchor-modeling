"""
Anchor Modeling - SQLMesh Python Blueprint

Generates all anchor modeling entities (anchors, ties, attributes, knots)
from model.xml (structure) and sources.yaml (source mappings).
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml
import sqlglot
from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent / "config.yaml"
MODEL_XML = Path(__file__).parent / "model.xml"
SOURCES_YAML = Path(__file__).parent / "sources.yaml"

# Load configuration
with open(_CONFIG_PATH) as f:
    _config = yaml.safe_load(f)

TARGET_DATABASE = _config["target_database"]
TARGET_SCHEMA = _config["target_schema"]
OUTPUT_CASE_STYLE = _config["output_case_style"]


# ---------------------------------------------------------------------------
# Column Name Conversion
# ---------------------------------------------------------------------------


def _camel_to_snake(name: str) -> str:
    """
    Convert camelCase to snake_case with special handling for underscores.

    Rule: If the camelCase name contains underscores (e.g., "bool_isCamelCase"),
    preserve them by doubling them in the output (e.g., "bool__is_camel_case").

    Examples:
        orderId -> order_id
        customerId -> customer_id
        bool_isCamelCase -> bool__is_camel_case
        EM_reports -> em__reports
    """
    import re

    # First, identify any existing underscores and mark them for doubling
    # Replace _ with a placeholder that won't conflict
    name = name.replace('_', '___UNDERSCORE___')

    # Insert underscores before uppercase letters
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)

    # Convert to lowercase
    name = name.lower()

    # Replace the placeholder with double underscores
    name = name.replace('___underscore___', '__')

    return name


def _format_column_name(name: str) -> str:
    """
    Format column name according to OUTPUT_CASE_STYLE.

    Assumes input is in camelCase (matching sources.yaml).
    """
    if OUTPUT_CASE_STYLE == "snake_case":
        return _camel_to_snake(name)
    else:  # camelCase
        return name


# ---------------------------------------------------------------------------
# Model Loading & Validation
# ---------------------------------------------------------------------------


class ModelValidationError(Exception):
    """Raised when model structure or sources are invalid."""
    pass


def _parse_xml_structure(xml_path: Path) -> dict[str, Any]:
    """
    Parse model.xml for anchor model structure.
    Returns: {anchors: {mnemonic: {descriptor, ...}}, ties: {name: {roles, ...}}}
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    model_structure = {"anchors": {}, "ties": {}}

    # Parse anchors
    for anchor_elem in root.findall("anchor"):
        mnemonic = anchor_elem.get("mnemonic")
        descriptor = anchor_elem.get("descriptor")
        model_structure["anchors"][mnemonic] = {
            "mnemonic": mnemonic,
            "descriptor": descriptor,
        }

    # Parse ties
    for tie_elem in root.findall("tie"):
        roles = []
        for role_elem in tie_elem.findall("anchorRole"):
            roles.append({
                "type": role_elem.get("type"),
                "role": role_elem.get("role"),
                "identifier": role_elem.get("identifier") == "true",
            })

        # Build tie name from roles
        tie_name = _build_tie_name(roles)
        model_structure["ties"][tie_name] = {"roles": roles}

    return model_structure


def _build_tie_name(roles: list[dict[str, Any]]) -> str:
    """Build canonical tie name from roles."""
    sorted_roles = sorted(roles, key=lambda r: (not r.get("identifier", False), r["type"]))
    parts = []
    for r in sorted_roles:
        parts.extend([r["type"], r["role"]])
    return "_".join(parts)


def _load_sources(sources_path: Path) -> dict[str, Any]:
    """Load source mappings from sources.yaml."""
    if not sources_path.exists():
        return {"anchors": {}, "ties": {}}
    with open(sources_path) as f:
        return yaml.safe_load(f) or {"anchors": {}, "ties": {}}


def _load_model(
    xml_path: Path = MODEL_XML,
    sources_path: Path = SOURCES_YAML,
) -> dict[str, Any]:
    """
    Load anchor model by combining structure from XML and sources from YAML.
    """
    structure = _parse_xml_structure(xml_path)
    sources = _load_sources(sources_path)

    # Merge sources into structure
    for mnemonic, anchor_data in structure["anchors"].items():
        anchor_data["sources"] = sources.get("anchors", {}).get(mnemonic, [])

    for tie_name, tie_data in structure["ties"].items():
        tie_data["sources"] = sources.get("ties", {}).get(tie_name, [])

    return structure


def _generate_anchor_stub(mnemonic: str, descriptor: str) -> str:
    """Generate YAML stub for a missing anchor source."""
    return f"""  {mnemonic}:  # {descriptor}
    - system: ???
      table: ???
      key: ???"""


def _generate_tie_stub(tie_name: str, roles: list[dict[str, Any]]) -> str:
    """Generate YAML stub for a missing tie source."""
    # Build keys section with role-specific or anchor-type keys
    anchor_counts: dict[str, int] = {}
    for r in roles:
        anchor_counts[r["type"]] = anchor_counts.get(r["type"], 0) + 1

    keys_lines = []
    for r in roles:
        anchor_type = r["type"]
        if anchor_counts[anchor_type] > 1:
            # Multiple roles for same anchor type - use role-specific key
            keys_lines.append(f"        {anchor_type}_{r['role']}: ???")
        else:
            # Single role for this anchor type
            keys_lines.append(f"        {anchor_type}: ???")

    keys_section = "\n".join(keys_lines)

    return f"""  {tie_name}:
    - system: ???
      table: ???
      keys:
{keys_section}"""


def _validate_anchor_sources(model_data: dict[str, Any]) -> list[str]:
    """Validate anchor sources and return list of error stubs."""
    required_fields = {"system", "table", "key"}
    stubs = []

    for mnemonic, config in model_data.get("anchors", {}).items():
        sources = config.get("sources", [])
        descriptor = config.get("descriptor", mnemonic)

        if not sources:
            stubs.append(_generate_anchor_stub(mnemonic, descriptor))
            continue

        for i, src in enumerate(sources):
            missing = required_fields - set(src.keys())
            if missing:
                stubs.append(
                    f"# Anchor {mnemonic} source[{i}] missing fields: {sorted(missing)}\n"
                    + _generate_anchor_stub(mnemonic, descriptor)
                )

    return stubs


def _validate_tie_sources(model_data: dict[str, Any]) -> list[str]:
    """Validate tie sources and return list of error stubs."""
    stubs = []

    for tie_name, config in model_data.get("ties", {}).items():
        sources = config.get("sources", [])
        roles = config.get("roles", [])

        if not sources:
            stubs.append(_generate_tie_stub(tie_name, roles))
            continue

        for i, src in enumerate(sources):
            missing_fields = []
            for field in ("system", "table", "keys"):
                if field not in src:
                    missing_fields.append(field)

            if missing_fields:
                stubs.append(
                    f"# Tie {tie_name} source[{i}] missing fields: {missing_fields}\n"
                    + _generate_tie_stub(tie_name, roles)
                )

    return stubs


def _validate_model(model_data: dict[str, Any]) -> None:
    """Validate the combined model structure and sources."""
    anchor_stubs = _validate_anchor_sources(model_data)
    tie_stubs = _validate_tie_sources(model_data)

    if anchor_stubs or tie_stubs:
        error_msg = ["Missing or incomplete source mappings in sources.yaml\n"]
        error_msg.append("Add these entries to sources.yaml:\n")

        if anchor_stubs:
            error_msg.append("anchors:")
            for stub in anchor_stubs:
                error_msg.append(stub)
            error_msg.append("")

        if tie_stubs:
            error_msg.append("ties:")
            for stub in tie_stubs:
                error_msg.append(stub)

        raise ModelValidationError("\n".join(error_msg))


# ---------------------------------------------------------------------------
# SQL Generation Helpers
# ---------------------------------------------------------------------------


def _build_keyset_expression(
    descriptor: str,
    system: str,
    key: str | list[str],
    tenant: str | None = None,
) -> exp.Expression:
    """
    Build keyset ID expression: {descriptor}@{system}[~{tenant}]|{key_values}
    """
    prefix = f"{descriptor}@{system}~{tenant}|" if tenant else f"{descriptor}@{system}|"
    keys = [key] if isinstance(key, str) else key

    parts: list[exp.Expression] = [exp.Literal.string(prefix)]
    for i, k in enumerate(keys):
        if i > 0:
            parts.append(exp.Literal.string("|"))
        parts.append(exp.Cast(this=exp.Column(this=exp.to_identifier(k)), to=exp.DataType.build("VARCHAR")))

    result = parts[0]
    for part in parts[1:]:
        result = exp.Concat(expressions=[result, part])
    return result


def _union_all(selects: list[exp.Select]) -> exp.Expression:
    """Combine multiple SELECTs with UNION ALL."""
    if len(selects) == 1:
        return selects[0]

    result = selects[0]
    for select in selects[1:]:
        result = exp.Union(this=result, expression=select, distinct=False)
    return result


def _build_incremental_query(
    source_query: exp.Expression,
    model_name: str,
    unique_keys: list[str],
    loaded_at_col: str,
    output_columns: list[tuple[str, str]],
) -> exp.Expression:
    """
    Build incremental query with anti-join pattern.

    Common pattern for all anchor model entities:
        WITH target AS (SELECT keys FROM model QUALIFY ROW_NUMBER() = 1),
             source AS (source_query)
        SELECT columns FROM source ANTI JOIN target ON keys
    """
    key_columns = [exp.Column(this=exp.to_identifier(k)) for k in unique_keys]

    window = exp.Window(
        this=exp.RowNumber(),
        partition_by=key_columns,
        order=exp.Order(expressions=[exp.Ordered(this=exp.Column(this=exp.to_identifier(loaded_at_col)), desc=True)]),
    )

    # Build explicit table reference: schema.table_name
    full_table_name = f"{TARGET_SCHEMA}.{model_name}"
    target_select = (
        exp.select(*key_columns)
        .from_(full_table_name)
        .qualify(exp.EQ(this=window, expression=exp.Literal.number(1)))
    )

    # Build join condition
    join_conditions = [
        exp.EQ(
            this=exp.Column(this=exp.to_identifier(k), table=exp.to_identifier("source")),
            expression=exp.Column(this=exp.to_identifier(k), table=exp.to_identifier("target")),
        )
        for k in unique_keys
    ]
    join_on = join_conditions[0]
    for cond in join_conditions[1:]:
        join_on = exp.And(this=join_on, expression=cond)

    # Build output columns with explicit types
    outer_columns = [
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(col_name), table=exp.to_identifier("source")),
            to=exp.DataType.build(data_type),
        ).as_(col_name)
        for col_name, data_type in output_columns
    ]

    main_select = (
        exp.select(*outer_columns)
        .from_(exp.Table(this=exp.to_identifier("source")))
        .join(exp.Table(this=exp.to_identifier("target")), on=join_on, join_type="ANTI")
    )

    return main_select.with_("target", as_=target_select).with_("source", as_=source_query)


# ---------------------------------------------------------------------------
# Anchor Query Generation
# ---------------------------------------------------------------------------


def _build_anchor_select(mnemonic: str, descriptor: str, source: dict[str, Any], execution_ts: str) -> exp.Select:
    """Build SELECT for one anchor source."""
    system = source["system"]
    tenant = source.get("tenant")
    table = source["table"]
    key = source["key"]

    keyset_expr = _build_keyset_expression(descriptor, system, key, tenant)
    tenant_expr = exp.Literal.string(tenant) if tenant else exp.Null()
    loaded_at_expr = exp.cast(exp.Literal.string(execution_ts), exp.DataType.build("timestamp"))

    return (
        exp.select(
            keyset_expr.as_(f"{mnemonic}_id"),
            exp.Literal.string(system).as_(f"{mnemonic}_system"),
            tenant_expr.as_(f"{mnemonic}_tenant"),
            loaded_at_expr.as_(f"{mnemonic}_loaded_at"),
        )
        .from_(table)
    )


def _build_anchor_query(blueprint: dict[str, Any], execution_ts: str, model_name: str) -> exp.Expression:
    """Build incremental anchor query."""
    mnemonic = blueprint["mnemonic"]
    sources = blueprint["sources"]

    if not sources:
        raise ValueError(f"No sources defined for anchor {mnemonic}")

    id_col = f"{mnemonic}_id"
    loaded_at_col = f"{mnemonic}_loaded_at"
    selects = [_build_anchor_select(mnemonic, blueprint["descriptor"], src, execution_ts) for src in sources]

    return _build_incremental_query(
        source_query=_union_all(selects),
        model_name=model_name,
        unique_keys=[id_col],
        loaded_at_col=loaded_at_col,
        output_columns=[
            (id_col, "VARCHAR"),
            (f"{mnemonic}_system", "VARCHAR"),
            (f"{mnemonic}_tenant", "VARCHAR"),
            (loaded_at_col, "TIMESTAMP"),
        ],
    )


# ---------------------------------------------------------------------------
# Tie Query Generation
# ---------------------------------------------------------------------------


def _build_tie_unique_keys(roles: list[dict[str, Any]]) -> list[str]:
    """Build unique key column names from tie roles."""
    anchor_counts: dict[str, int] = {}
    for r in roles:
        anchor_counts[r["type"]] = anchor_counts.get(r["type"], 0) + 1

    unique_keys = []
    for r in roles:
        anchor_type = r["type"]
        if anchor_counts[anchor_type] > 1:
            unique_keys.append(f"{anchor_type}_{r['role']}_id")
        else:
            unique_keys.append(f"{anchor_type}_id")
    return unique_keys


def _build_tie_select(
    tie_name: str,
    roles: list[dict[str, Any]],
    source: dict[str, Any],
    anchor_descriptors: dict[str, str],
    execution_ts: str,
) -> exp.Select:
    """Build SELECT for one tie source."""
    system = source["system"]
    tenant = source.get("tenant")
    table = source["table"]
    keys_config = source["keys"]

    columns = []
    for role in roles:
        anchor_type = role["type"]
        role_name = role["role"]
        descriptor = anchor_descriptors[anchor_type]

        role_key = f"{anchor_type}_{role_name}"
        if role_key in keys_config:
            key = keys_config[role_key]
            col_name = f"{anchor_type}_{role_name}_id"
        elif anchor_type in keys_config:
            key = keys_config[anchor_type]
            col_name = f"{anchor_type}_id"
        else:
            raise ValueError(f"Tie {tie_name}: no key mapping for role {role_key} or {anchor_type}")

        keyset_expr = _build_keyset_expression(descriptor, system, key, tenant)
        columns.append(keyset_expr.as_(col_name))

    loaded_at_expr = exp.cast(exp.Literal.string(execution_ts), exp.DataType.build("timestamp"))
    columns.append(loaded_at_expr.as_("loaded_at"))

    return exp.select(*columns).from_(table)


def _build_tie_query(blueprint: dict[str, Any], execution_ts: str, model_name: str) -> exp.Expression:
    """Build incremental tie query."""
    tie_name = blueprint["name"]
    roles = blueprint["roles"]
    sources = blueprint["sources"]
    anchor_descriptors = blueprint["anchor_descriptors"]

    if not sources:
        raise ValueError(f"No sources defined for tie {tie_name}")

    unique_keys = _build_tie_unique_keys(roles)
    selects = [_build_tie_select(tie_name, roles, src, anchor_descriptors, execution_ts) for src in sources]

    return _build_incremental_query(
        source_query=_union_all(selects),
        model_name=model_name,
        unique_keys=unique_keys,
        loaded_at_col="loaded_at",
        output_columns=[(k, "VARCHAR") for k in unique_keys] + [("loaded_at", "TIMESTAMP")],
    )


# ---------------------------------------------------------------------------
# Blueprint Generation
# ---------------------------------------------------------------------------


def _get_blueprints() -> list[dict[str, Any]]:
    """Generate blueprint configurations for all anchor model entities."""
    model_data = _load_model()
    _validate_model(model_data)

    anchor_descriptors = {mnemonic: config["descriptor"] for mnemonic, config in model_data["anchors"].items()}

    blueprints = []

    # Anchors
    for mnemonic, config in model_data["anchors"].items():
        blueprints.append({
            "model_name": f"anchor__{mnemonic}",
            "entity_type": "anchor",
            "name": mnemonic,
            "mnemonic": mnemonic,
            "descriptor": config["descriptor"],
            "sources": config.get("sources", []),
        })

    # Ties
    for tie_name, config in model_data.get("ties", {}).items():
        blueprints.append({
            "model_name": f"tie__{tie_name}",
            "entity_type": "tie",
            "name": tie_name,
            "roles": config["roles"],
            "sources": config.get("sources", []),
            "unique_key": _build_tie_unique_keys(config["roles"]),
            "anchor_descriptors": anchor_descriptors,
        })

    return blueprints


def _build_query(blueprint: dict[str, Any], execution_ts: str, model_name: str) -> exp.Expression:
    """Build query for any anchor model entity type."""
    entity_type = blueprint["entity_type"]

    if entity_type == "anchor":
        return _build_anchor_query(blueprint, execution_ts, model_name)
    elif entity_type == "tie":
        return _build_tie_query(blueprint, execution_ts, model_name)
    else:
        raise ValueError(f"Unknown entity type: {entity_type}")


# ---------------------------------------------------------------------------
# SQLMesh Model Definition
# ---------------------------------------------------------------------------

_blueprint_data = _get_blueprints()
_configs = {bp["model_name"]: bp for bp in _blueprint_data}


@model(
    f"{TARGET_SCHEMA}.@{{model_name}}",
    is_sql=True,
    kind={"name": ModelKindName.INCREMENTAL_UNMANAGED},
    blueprints=_blueprint_data,
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """Generate entity query at runtime."""
    model_name = evaluator.blueprint_var("model_name")
    blueprint = _configs[model_name]
    execution_ts = evaluator.locals["execution_tstz"]
    return _build_query(blueprint, execution_ts, model_name)

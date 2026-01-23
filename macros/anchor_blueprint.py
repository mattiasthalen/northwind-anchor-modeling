"""
Anchor Model Blueprint Generator

Reads the unified model.yaml (synced from model.xml) and generates
SQLMesh Python model blueprints using sqlglot.
"""

from pathlib import Path
from typing import Any

import yaml
from sqlglot import exp


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

METADATA_DIR = Path(__file__).parent.parent / "metadata"
MODEL_YAML = METADATA_DIR / "model.yaml"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def load_model(path: Path = MODEL_YAML) -> dict[str, Any]:
    """
    Load the unified model.yaml.

    Returns:
        dict with 'anchors' and 'ties' keys
    """
    with open(path) as f:
        return yaml.safe_load(f)


def build_tie_name(roles: list[dict[str, Any]]) -> str:
    """
    Build canonical tie name from roles.

    Format: {type1}_{role1}_{type2}_{role2}
    The identifier=true role comes first.
    """
    sorted_roles = sorted(roles, key=lambda r: (not r["identifier"], r["type"]))
    parts = []
    for r in sorted_roles:
        parts.extend([r["type"], r["role"]])
    return "_".join(parts)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ModelValidationError(Exception):
    """Raised when model.yaml is invalid or incomplete."""
    pass


def validate_anchor_sources(model: dict[str, Any]) -> None:
    """
    Validate each anchor has required source fields.

    Raises:
        ModelValidationError: If required fields are missing
    """
    required_fields = {"system", "table", "key"}  # tenant is optional

    for mnemonic, config in model.get("anchors", {}).items():
        sources = config.get("sources", [])

        if not sources:
            raise ModelValidationError(
                f"Anchor {mnemonic} has no sources defined"
            )

        for i, src in enumerate(sources):
            missing = required_fields - set(src.keys())
            if missing:
                raise ModelValidationError(
                    f"Anchor {mnemonic} source[{i}] missing required fields: {sorted(missing)}"
                )


def validate_tie_sources(model: dict[str, Any]) -> None:
    """
    Validate each tie has required source fields.

    Raises:
        ModelValidationError: If required fields are missing
    """
    for tie_name, config in model.get("ties", {}).items():
        sources = config.get("sources", [])

        if not sources:
            raise ModelValidationError(f"Tie {tie_name} has no sources defined")

        for i, src in enumerate(sources):
            if "system" not in src:
                raise ModelValidationError(
                    f"Tie {tie_name} source[{i}] missing 'system'"
                )
            if "table" not in src:
                raise ModelValidationError(
                    f"Tie {tie_name} source[{i}] missing 'table'"
                )
            if "keys" not in src:
                raise ModelValidationError(
                    f"Tie {tie_name} source[{i}] missing 'keys'"
                )


def validate_model(model: dict[str, Any]) -> None:
    """
    Validate the unified model.yaml.

    Raises:
        ModelValidationError: If validation fails
    """
    validate_anchor_sources(model)
    validate_tie_sources(model)


# ---------------------------------------------------------------------------
# SQL Generation (sqlglot)
# ---------------------------------------------------------------------------

def build_keyset_expression(
    descriptor: str,
    system: str,
    key: str | list[str],
    tenant: str | None = None,
) -> exp.Expression:
    """
    Build the keyset ID expression.

    With tenant:    {descriptor}@{system}~{tenant}|{key_value(s)}
    Without tenant: {descriptor}@{system}|{key_value(s)}

    For composite keys, values are pipe-delimited.
    """
    if tenant:
        prefix = f"{descriptor}@{system}~{tenant}|"
    else:
        prefix = f"{descriptor}@{system}|"

    # Normalize key to list
    keys = [key] if isinstance(key, str) else key

    # Build concatenation: prefix || key1 || '|' || key2 || ...
    parts: list[exp.Expression] = [exp.Literal.string(prefix)]

    for i, k in enumerate(keys):
        if i > 0:
            parts.append(exp.Literal.string("|"))
        # Keys are already snake_case from model.yaml
        parts.append(
            exp.Cast(
                this=exp.Column(this=exp.to_identifier(k)),
                to=exp.DataType.build("VARCHAR"),
            )
        )

    # Build nested concat: concat(concat(prefix, key1), '|', key2, ...)
    result = parts[0]
    for part in parts[1:]:
        result = exp.Concat(expressions=[result, part])

    return result


def build_anchor_select(
    mnemonic: str,
    descriptor: str,
    source: dict[str, Any],
    execution_ts: str,
) -> exp.Select:
    """
    Build a SELECT statement for one anchor source.

    Returns columns:
        - {mnemonic}_ID: the keyset identifier
        - {mnemonic}_ID_SYSTEM: source system name
        - {mnemonic}_ID_TENANT: tenant identifier (NULL if not specified)
    """
    system = source["system"]
    tenant = source.get("tenant")  # optional
    table = source["table"]
    key = source["key"]

    keyset_expr = build_keyset_expression(descriptor, system, key, tenant)

    # Tenant column: string literal if present, NULL if not
    tenant_expr = (
        exp.Literal.string(tenant) if tenant
        else exp.Null()
    )

    # Execution timestamp as loaded_at
    loaded_at_expr = exp.cast(
        exp.Literal.string(execution_ts),
        exp.DataType.build("timestamp")
    )

    return (
        exp.select(
            keyset_expr.as_(f"{mnemonic}_id"),
            exp.Literal.string(system).as_(f"{mnemonic}_system"),
            tenant_expr.as_(f"{mnemonic}_tenant"),
            loaded_at_expr.as_(f"{mnemonic}_loaded_at"),
        )
        .from_(table)
    )


def build_anchor_query(
    mnemonic: str,
    descriptor: str,
    sources: list[dict[str, Any]],
    execution_ts: str,
    model_name: str,
) -> exp.Expression:
    """
    Build incremental anchor query with anti-join pattern.

    Structure:
        WITH target AS (
            SELECT {mnemonic}_id FROM model QUALIFY ROW_NUMBER() OVER (...) = 1
        ),
        source AS (
            SELECT ... FROM source_tables (UNION ALL if multiple)
        )
        SELECT source.* FROM source
        ANTI JOIN target ON {mnemonic}_id
    """
    if not sources:
        raise ValueError(f"No sources defined for anchor {mnemonic}")

    # Column names for this anchor
    id_col = f"{mnemonic}_id"
    system_col = f"{mnemonic}_system"
    tenant_col = f"{mnemonic}_tenant"
    loaded_at_col = f"{mnemonic}_loaded_at"

    # Build source selects (UNION ALL if multiple)
    selects = [build_anchor_select(mnemonic, descriptor, src, execution_ts) for src in sources]

    if len(selects) == 1:
        source_query = selects[0]
    else:
        source_query = selects[0]
        for select in selects[1:]:
            source_query = exp.Union(this=source_query, expression=select, distinct=False)

    # Build target CTE: SELECT id_col FROM model QUALIFY ROW_NUMBER() = 1
    window = exp.Window(
        this=exp.RowNumber(),
        partition_by=[exp.Column(this=exp.to_identifier(id_col))],
        order=exp.Order(expressions=[exp.Ordered(this=exp.Column(this=exp.to_identifier(loaded_at_col)), desc=True)]),
    )

    target_select = (
        exp.select(exp.Column(this=exp.to_identifier(id_col)))
        .from_(model_name)
        .qualify(exp.EQ(this=window, expression=exp.Literal.number(1)))
    )

    # Build join condition
    join_on = exp.EQ(
        this=exp.Column(this=exp.to_identifier(id_col), table=exp.to_identifier("source")),
        expression=exp.Column(this=exp.to_identifier(id_col), table=exp.to_identifier("target")),
    )

    # Select explicit columns with types for self-referencing model inference
    outer_columns = [
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(id_col), table=exp.to_identifier("source")),
            to=exp.DataType.build("VARCHAR"),
        ).as_(id_col),
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(system_col), table=exp.to_identifier("source")),
            to=exp.DataType.build("VARCHAR"),
        ).as_(system_col),
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(tenant_col), table=exp.to_identifier("source")),
            to=exp.DataType.build("VARCHAR"),
        ).as_(tenant_col),
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(loaded_at_col), table=exp.to_identifier("source")),
            to=exp.DataType.build("TIMESTAMP"),
        ).as_(loaded_at_col),
    ]

    main_select = (
        exp.select(*outer_columns)
        .from_(exp.Table(this=exp.to_identifier("source")))
        .join(
            exp.Table(this=exp.to_identifier("target")),
            on=join_on,
            join_type="ANTI",
        )
    )

    # Combine with both CTEs
    return main_select.with_("target", as_=target_select).with_("source", as_=source_query)


# ---------------------------------------------------------------------------
# Tie SQL Generation
# ---------------------------------------------------------------------------

def build_tie_keyset_expression(
    descriptor: str,
    system: str,
    key: str | list[str],
    tenant: str | None = None,
) -> exp.Expression:
    """
    Build keyset ID expression for a tie role.
    """
    if tenant:
        prefix = f"{descriptor}@{system}~{tenant}|"
    else:
        prefix = f"{descriptor}@{system}|"

    keys = [key] if isinstance(key, str) else key

    parts: list[exp.Expression] = [exp.Literal.string(prefix)]

    for i, k in enumerate(keys):
        if i > 0:
            parts.append(exp.Literal.string("|"))
        # Keys are already snake_case from model.yaml
        parts.append(
            exp.Cast(
                this=exp.Column(this=exp.to_identifier(k)),
                to=exp.DataType.build("VARCHAR"),
            )
        )

    result = parts[0]
    for part in parts[1:]:
        result = exp.Concat(expressions=[result, part])

    return result


def build_tie_select(
    tie_name: str,
    roles: list[dict[str, Any]],
    source: dict[str, Any],
    anchor_descriptors: dict[str, str],
    execution_ts: str,
) -> exp.Select:
    """
    Build a SELECT statement for one tie source.

    Returns columns for each anchor ID plus loaded_at.
    """
    system = source["system"]
    tenant = source.get("tenant")
    table = source["table"]
    keys_config = source["keys"]

    columns = []

    # For each role, build the keyset expression
    for role in roles:
        anchor_type = role["type"]
        role_name = role["role"]
        descriptor = anchor_descriptors[anchor_type]

        # Handle self-referencing ties: key might be like "EM_to" or "EM_reports"
        role_key = f"{anchor_type}_{role_name}"
        if role_key in keys_config:
            key = keys_config[role_key]
            col_name = f"{anchor_type}_{role_name}_id"
        elif anchor_type in keys_config:
            key = keys_config[anchor_type]
            col_name = f"{anchor_type}_id"
        else:
            raise ValueError(
                f"Tie {tie_name}: no key mapping for role {role_key} or {anchor_type}"
            )

        keyset_expr = build_tie_keyset_expression(descriptor, system, key, tenant)
        columns.append(keyset_expr.as_(col_name))

    # Add loaded_at
    loaded_at_expr = exp.cast(
        exp.Literal.string(execution_ts),
        exp.DataType.build("timestamp")
    )
    columns.append(loaded_at_expr.as_("loaded_at"))

    return exp.select(*columns).from_(table)


def build_tie_query(
    tie_name: str,
    roles: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    anchor_descriptors: dict[str, str],
    execution_ts: str,
    model_name: str,
) -> exp.Expression:
    """
    Build incremental tie query with anti-join pattern.

    Structure:
        WITH target AS (
            SELECT key_cols FROM model QUALIFY ROW_NUMBER() OVER (...) = 1
        ),
        source AS (
            SELECT ... FROM source_tables
        )
        SELECT source.* FROM source
        LEFT JOIN target ON key_cols
        WHERE target.key IS NULL
    """
    if not sources:
        raise ValueError(f"No sources defined for tie {tie_name}")

    # Build unique key column names from roles
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

    # Build source selects (UNION ALL if multiple)
    selects = [build_tie_select(tie_name, roles, src, anchor_descriptors, execution_ts) for src in sources]

    if len(selects) == 1:
        source_query = selects[0]
    else:
        source_query = selects[0]
        for select in selects[1:]:
            source_query = exp.Union(this=source_query, expression=select, distinct=False)

    # Build target CTE: SELECT key_cols FROM model QUALIFY ROW_NUMBER() = 1
    key_columns = [exp.Column(this=exp.to_identifier(k)) for k in unique_keys]

    window = exp.Window(
        this=exp.RowNumber(),
        partition_by=key_columns,
        order=exp.Order(expressions=[exp.Ordered(this=exp.Column(this=exp.to_identifier("loaded_at")), desc=True)]),
    )

    target_select = (
        exp.select(*key_columns)
        .from_(model_name)
        .qualify(exp.EQ(this=window, expression=exp.Literal.number(1)))
    )

    # Build main select with anti-join using two CTEs
    join_conditions = [
        exp.EQ(
            this=exp.Column(this=exp.to_identifier(k), table=exp.to_identifier("source")),
            expression=exp.Column(this=exp.to_identifier(k), table=exp.to_identifier("target")),
        )
        for k in unique_keys
    ]

    if len(join_conditions) == 1:
        join_on = join_conditions[0]
    else:
        join_on = join_conditions[0]
        for cond in join_conditions[1:]:
            join_on = exp.And(this=join_on, expression=cond)

    # Select explicit columns with types for self-referencing model inference
    outer_columns = [
        exp.Cast(
            this=exp.Column(this=exp.to_identifier(k), table=exp.to_identifier("source")),
            to=exp.DataType.build("VARCHAR"),
        ).as_(k)
        for k in unique_keys
    ]
    outer_columns.append(
        exp.Cast(
            this=exp.Column(this=exp.to_identifier("loaded_at"), table=exp.to_identifier("source")),
            to=exp.DataType.build("TIMESTAMP"),
        ).as_("loaded_at")
    )

    main_select = (
        exp.select(*outer_columns)
        .from_(exp.Table(this=exp.to_identifier("source")))
        .join(
            exp.Table(this=exp.to_identifier("target")),
            on=join_on,
            join_type="ANTI",
        )
    )

    # Combine with both CTEs
    return main_select.with_("target", as_=target_select).with_("source", as_=source_query)


# ---------------------------------------------------------------------------
# Blueprint Generation
# ---------------------------------------------------------------------------

def get_anchor_blueprints() -> list[dict[str, Any]]:
    """
    Generate blueprint configurations for all anchors.

    Returns:
        List of dicts with keys: mnemonic, descriptor, sources
        (sources is raw config, query built at runtime to avoid serialization issues)
    """
    model = load_model()
    validate_anchor_sources(model)

    blueprints = []
    for mnemonic, config in model["anchors"].items():
        blueprints.append({
            "mnemonic": mnemonic,
            "descriptor": config["descriptor"],
            "sources": config.get("sources", []),
        })

    return blueprints


def get_tie_blueprints() -> list[dict[str, Any]]:
    """
    Generate blueprint configurations for all ties.

    Returns:
        List of dicts with keys: tie_name, roles, sources, unique_key, anchor_descriptors
    """
    model = load_model()
    validate_tie_sources(model)

    # Build mnemonic -> descriptor lookup
    anchor_descriptors = {
        mnemonic: config["descriptor"]
        for mnemonic, config in model["anchors"].items()
    }

    blueprints = []
    for tie_name, config in model["ties"].items():
        roles = config["roles"]
        sources = config.get("sources", [])

        # Build unique key columns based on roles
        # For self-referencing ties, include role in column name
        unique_keys = []
        anchor_counts: dict[str, int] = {}
        for r in roles:
            anchor_counts[r["type"]] = anchor_counts.get(r["type"], 0) + 1

        for r in roles:
            anchor_type = r["type"]
            if anchor_counts[anchor_type] > 1:
                # Self-referencing: use {type}_{role}_id
                unique_keys.append(f"{anchor_type}_{r['role']}_id")
            else:
                unique_keys.append(f"{anchor_type}_id")

        # Build columns dict for self-referencing model
        columns = {k: "VARCHAR" for k in unique_keys}
        columns["loaded_at"] = "TIMESTAMP"

        blueprints.append({
            "tie_name": tie_name,
            "roles": roles,
            "sources": sources,
            "unique_key": unique_keys,
            "columns": columns,
            "anchor_descriptors": anchor_descriptors,
        })

    return blueprints



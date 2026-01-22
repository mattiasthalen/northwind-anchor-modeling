"""
Anchor Model Blueprint Generator

Parses anchor model XML and source manifest YAML to generate
SQLMesh Python model blueprints using sqlglot.
"""

from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import yaml
from sqlglot import exp


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

METADATA_DIR = Path(__file__).parent.parent / "metadata"
MODEL_XML = METADATA_DIR / "model.xml"
SOURCES_YAML = METADATA_DIR / "sources.yaml"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_anchor_model(path: Path = MODEL_XML) -> dict[str, dict[str, Any]]:
    """
    Parse anchor model XML and extract anchor definitions.

    Returns:
        dict mapping mnemonic -> {descriptor, identity, ...}
    """
    tree = ET.parse(path)
    root = tree.getroot()

    anchors = {}
    for anchor_elem in root.findall("anchor"):
        mnemonic = anchor_elem.get("mnemonic")
        anchors[mnemonic] = {
            "descriptor": anchor_elem.get("descriptor"),
            "identity": anchor_elem.get("identity", "int"),
        }

    return anchors


def parse_sources(path: Path = SOURCES_YAML) -> dict[str, Any]:
    """
    Parse source manifest YAML.

    Returns:
        dict with 'anchors' key containing source mappings
    """
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ManifestValidationError(Exception):
    """Raised when source manifest doesn't match anchor model."""
    pass


def validate_manifest(xml_anchors: set[str], manifest_anchors: set[str]) -> None:
    """
    Validate 1:1 correspondence between XML anchors and source manifest.

    Raises:
        ManifestValidationError: If anchors don't match
    """
    # Anchors in XML but missing from manifest
    orphaned = xml_anchors - manifest_anchors
    if orphaned:
        raise ManifestValidationError(
            f"Anchors in XML have no source mapping: {sorted(orphaned)}"
        )

    # Anchors in manifest but not in XML
    dangling = manifest_anchors - xml_anchors
    if dangling:
        raise ManifestValidationError(
            f"Source manifest references unknown anchors: {sorted(dangling)}"
        )


def validate_sources(manifest: dict[str, Any]) -> None:
    """
    Validate each source entry has required fields.

    Raises:
        ManifestValidationError: If required fields are missing
    """
    required_fields = {"system", "table", "key"}  # tenant is optional

    for mnemonic, config in manifest.get("anchors", {}).items():
        sources = config.get("sources", [])

        if not sources:
            raise ManifestValidationError(
                f"Anchor {mnemonic} has no sources defined"
            )

        for i, src in enumerate(sources):
            missing = required_fields - set(src.keys())
            if missing:
                raise ManifestValidationError(
                    f"Anchor {mnemonic} source[{i}] missing required fields: {sorted(missing)}"
                )


def validate_all() -> tuple[dict[str, dict], dict[str, Any]]:
    """
    Parse and validate both configs. Returns them if valid.

    Returns:
        tuple of (xml_anchors, manifest)

    Raises:
        ManifestValidationError: If validation fails
    """
    xml_anchors = parse_anchor_model()
    manifest = parse_sources()

    validate_manifest(
        xml_anchors=set(xml_anchors.keys()),
        manifest_anchors=set(manifest.get("anchors", {}).keys()),
    )
    validate_sources(manifest)

    return xml_anchors, manifest


# ---------------------------------------------------------------------------
# SQL Generation (sqlglot)
# ---------------------------------------------------------------------------

import re


def to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase to snake_case."""
    # Insert underscore before uppercase letters and lowercase the result
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def build_keyset_expression(
    mnemonic: str,
    system: str,
    key: str | list[str],
    tenant: str | None = None,
) -> exp.Expression:
    """
    Build the keyset ID expression.

    With tenant:    {mnemonic}@{system}~{tenant}|{key_value(s)}
    Without tenant: {mnemonic}@{system}|{key_value(s)}

    For composite keys, values are pipe-delimited.
    """
    if tenant:
        prefix = f"{mnemonic}@{system}~{tenant}|"
    else:
        prefix = f"{mnemonic}@{system}|"

    # Normalize key to list
    keys = [key] if isinstance(key, str) else key

    # Build concatenation: prefix || key1 || '|' || key2 || ...
    parts: list[exp.Expression] = [exp.Literal.string(prefix)]

    for i, k in enumerate(keys):
        if i > 0:
            parts.append(exp.Literal.string("|"))
        # Convert key to snake_case and cast to varchar for concatenation
        snake_key = to_snake_case(k)
        parts.append(
            exp.Cast(
                this=exp.Column(this=exp.to_identifier(snake_key)),
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

    keyset_expr = build_keyset_expression(mnemonic, system, key, tenant)

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
    sources: list[dict[str, Any]],
    execution_ts: str,
) -> exp.Expression:
    """
    Build UNION ALL query for all sources of an anchor.
    """
    if not sources:
        raise ValueError(f"No sources defined for anchor {mnemonic}")

    selects = [build_anchor_select(mnemonic, src, execution_ts) for src in sources]

    if len(selects) == 1:
        return selects[0]

    # UNION ALL all selects
    result = selects[0]
    for select in selects[1:]:
        result = exp.Union(this=result, expression=select, distinct=False)

    return result


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
    xml_anchors, manifest = validate_all()

    blueprints = []
    for mnemonic, xml_config in xml_anchors.items():
        sources = manifest["anchors"][mnemonic]["sources"]

        blueprints.append({
            "mnemonic": mnemonic,
            "descriptor": xml_config["descriptor"],
            "sources": sources,  # raw config, not pre-built query
        })

    return blueprints

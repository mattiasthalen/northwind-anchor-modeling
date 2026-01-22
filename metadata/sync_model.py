"""
Anchor Model Sync Tool

True bidirectional sync: XML ↔ YAML

The sources are stored in the <description> element of each anchor/tie in the XML.
This way the official Anchor Modeler tool preserves them, and we can edit them.

Workflow:
1. Design model in Anchor Modeler → model.xml
2. Run sync → generates model.yaml (full export)
3. Edit sources in model.yaml
4. Run sync → writes sources back to XML <description> elements

Files:
- model.xml  : Model from Anchor Modeler (sources stored in <description>)
- model.yaml : Full YAML export (edit sources here, sync writes them back to XML)
"""

from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import yaml


# ---------------------------------------------------------------------------
# Paths (relative to script location)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
MODEL_XML = SCRIPT_DIR / "model.xml"
MODEL_YAML = SCRIPT_DIR / "model.yaml"


# ---------------------------------------------------------------------------
# XML Parsing Helpers
# ---------------------------------------------------------------------------

def parse_value(value: str) -> Any:
    """Convert string value to appropriate type."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def element_to_dict(elem: ET.Element) -> dict[str, Any]:
    """Convert XML element attributes to dict."""
    result = {}
    for key, value in elem.attrib.items():
        result[key] = parse_value(value)
    return result


def parse_description_sources(elem: ET.Element) -> list[dict[str, Any]]:
    """Parse sources from <description> element's <source> children."""
    desc_elem = elem.find("description")
    if desc_elem is None:
        return []

    sources = []
    for source_elem in desc_elem.findall("source"):
        source = {}
        for key, value in source_elem.attrib.items():
            source[key] = value

        # Parse nested <key> elements for composite keys
        key_elems = source_elem.findall("key")
        if key_elems:
            if len(key_elems) == 1 and not key_elems[0].findall("col"):
                source["key"] = key_elems[0].text
            else:
                # Composite key
                keys = []
                for key_elem in key_elems:
                    col_elems = key_elem.findall("col")
                    if col_elems:
                        keys = [col.text for col in col_elems]
                    elif key_elem.text:
                        keys.append(key_elem.text)
                source["key"] = keys

        # Parse nested <keys> for tie key mappings
        keys_elem = source_elem.find("keys")
        if keys_elem is not None:
            key_map = {}
            for key_elem in keys_elem:
                anchor = key_elem.tag
                col_elems = key_elem.findall("col")
                if col_elems:
                    key_map[anchor] = [col.text for col in col_elems]
                elif key_elem.text:
                    key_map[anchor] = key_elem.text
            source["keys"] = key_map

        sources.append(source)

    return sources


# ---------------------------------------------------------------------------
# Tie Naming
# ---------------------------------------------------------------------------

def build_tie_name(roles: list[dict[str, Any]]) -> str:
    """Build canonical tie name from roles."""
    sorted_roles = sorted(roles, key=lambda r: (not r.get("identifier", False), r["type"]))
    parts = []
    for r in sorted_roles:
        parts.extend([r["type"], r["role"]])
    return "_".join(parts)


# ---------------------------------------------------------------------------
# XML Parsing (Full)
# ---------------------------------------------------------------------------

def parse_xml_full(path: Path) -> dict[str, Any]:
    """
    Parse full XML structure to dict.
    Sources are read from <description> elements.
    """
    tree = ET.parse(path)
    root = tree.getroot()

    model = {
        "schema": element_to_dict(root),
        "anchors": {},
        "ties": {},
    }

    # Parse schema-level metadata
    schema_meta = root.find("metadata")
    if schema_meta is not None:
        model["schema"]["metadata"] = element_to_dict(schema_meta)

    # Parse anchors
    for anchor_elem in root.findall("anchor"):
        mnemonic = anchor_elem.get("mnemonic")
        anchor_data = element_to_dict(anchor_elem)

        # Parse nested elements
        meta = anchor_elem.find("metadata")
        if meta is not None:
            anchor_data["metadata"] = element_to_dict(meta)

        layout = anchor_elem.find("layout")
        if layout is not None:
            anchor_data["layout"] = element_to_dict(layout)

        # Parse sources (stored in XML <description> element)
        anchor_data["sources"] = parse_description_sources(anchor_elem)

        model["anchors"][mnemonic] = anchor_data

    # Parse ties
    for tie_elem in root.findall("tie"):
        tie_data = element_to_dict(tie_elem)

        # Parse anchor roles
        roles = []
        for role_elem in tie_elem.findall("anchorRole"):
            roles.append(element_to_dict(role_elem))
        tie_data["roles"] = roles

        # Parse nested elements
        meta = tie_elem.find("metadata")
        if meta is not None:
            tie_data["metadata"] = element_to_dict(meta)

        layout = tie_elem.find("layout")
        if layout is not None:
            tie_data["layout"] = element_to_dict(layout)

        # Parse sources (stored in XML <description> element on tie or first anchorRole)
        sources = parse_description_sources(tie_elem)
        if not sources:
            first_role = tie_elem.find("anchorRole")
            if first_role is not None:
                sources = parse_description_sources(first_role)

        tie_data["sources"] = sources

        tie_name = build_tie_name(roles)
        model["ties"][tie_name] = tie_data

    return model


# ---------------------------------------------------------------------------
# YAML Loading
# ---------------------------------------------------------------------------

def load_model_yaml(path: Path = MODEL_YAML) -> dict[str, Any]:
    """Load existing model.yaml."""
    if not path.exists():
        return {"anchors": {}, "ties": {}}
    with open(path) as f:
        return yaml.safe_load(f) or {"anchors": {}, "ties": {}}


# ---------------------------------------------------------------------------
# Write Sources Back to XML
# ---------------------------------------------------------------------------

def build_source_xml(parent: ET.Element, sources: list[dict[str, Any]]) -> None:
    """Build <source> XML elements under parent."""
    # Clear existing source elements
    for old in parent.findall("source"):
        parent.remove(old)

    for src in sources:
        source_elem = ET.SubElement(parent, "source")
        source_elem.set("system", src.get("system", ""))
        source_elem.set("table", src.get("table", ""))

        # Handle anchor key (single or composite)
        if "key" in src:
            key = src["key"]
            if isinstance(key, list):
                key_elem = ET.SubElement(source_elem, "key")
                for col in key:
                    col_elem = ET.SubElement(key_elem, "col")
                    col_elem.text = col
            else:
                key_elem = ET.SubElement(source_elem, "key")
                key_elem.text = key

        # Handle tie keys mapping
        if "keys" in src:
            keys_elem = ET.SubElement(source_elem, "keys")
            for anchor, cols in src["keys"].items():
                anchor_elem = ET.SubElement(keys_elem, anchor)
                if isinstance(cols, list):
                    for col in cols:
                        col_elem = ET.SubElement(anchor_elem, "col")
                        col_elem.text = col
                else:
                    anchor_elem.text = cols


def write_sources_to_xml(
    xml_path: Path,
    model: dict[str, Any],
) -> None:
    """
    Write sources back to XML <description><source> elements.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Update anchor descriptions
    for anchor_elem in root.findall("anchor"):
        mnemonic = anchor_elem.get("mnemonic")
        if mnemonic not in model["anchors"]:
            continue

        sources = model["anchors"][mnemonic].get("sources", [])
        if not sources:
            continue

        # Find or create <description> element
        desc_elem = anchor_elem.find("description")
        if desc_elem is None:
            desc_elem = ET.SubElement(anchor_elem, "description")

        # Clear text content, write as XML elements
        desc_elem.text = None
        build_source_xml(desc_elem, sources)

    # Update tie descriptions
    for tie_elem in root.findall("tie"):
        # Build tie name from roles
        roles = []
        for role_elem in tie_elem.findall("anchorRole"):
            roles.append({
                "role": role_elem.get("role"),
                "type": role_elem.get("type"),
                "identifier": role_elem.get("identifier") == "true",
            })
        tie_name = build_tie_name(roles)

        if tie_name not in model["ties"]:
            continue

        sources = model["ties"][tie_name].get("sources", [])
        if not sources:
            continue

        # Find or create <description> on first anchorRole
        first_role = tie_elem.find("anchorRole")
        if first_role is not None:
            desc_elem = first_role.find("description")
            if desc_elem is None:
                desc_elem = ET.SubElement(first_role, "description")

            desc_elem.text = None
            build_source_xml(desc_elem, sources)

    # Pretty print and write back to XML
    indent_xml(root)
    tree.write(xml_path, encoding="unicode")


def indent_xml(elem: ET.Element, level: int = 0) -> None:
    """Add indentation to XML elements for pretty printing."""
    indent = "\n" + "  " * level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent


# ---------------------------------------------------------------------------
# Sync Logic
# ---------------------------------------------------------------------------

def sync_model(
    xml_path: Path = MODEL_XML,
    yaml_path: Path = MODEL_YAML,
) -> dict[str, Any]:
    """
    Bidirectional sync:
    1. Read XML structure + sources from <description>
    2. Merge with any sources edited in model.yaml
    3. Write model.yaml
    4. Write sources back to XML <description>
    """
    # Parse XML (sources from <description>)
    model = parse_xml_full(xml_path)

    # Load existing YAML to get edited sources
    existing_yaml = load_model_yaml(yaml_path)

    # Merge: YAML sources take precedence (user edits there)
    for mnemonic, anchor_data in model["anchors"].items():
        if mnemonic in existing_yaml.get("anchors", {}):
            yaml_sources = existing_yaml["anchors"][mnemonic].get("sources", [])
            if yaml_sources:
                anchor_data["sources"] = yaml_sources

    for tie_name, tie_data in model["ties"].items():
        if tie_name in existing_yaml.get("ties", {}):
            yaml_sources = existing_yaml["ties"][tie_name].get("sources", [])
            if yaml_sources:
                tie_data["sources"] = yaml_sources

    # Write YAML
    with open(yaml_path, "w") as f:
        yaml.dump(model, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Write sources back to XML <description> elements
    write_sources_to_xml(xml_path, model)

    # Stats
    missing_anchor_sources = [m for m, a in model["anchors"].items() if not a.get("sources")]
    missing_tie_sources = [t for t, c in model["ties"].items() if not c.get("sources")]

    return {
        "model": model,
        "missing_anchor_sources": missing_anchor_sources,
        "missing_tie_sources": missing_tie_sources,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Run sync from command line."""
    print("Bidirectional sync:")
    print(f"  XML:  {MODEL_XML}")
    print(f"  YAML: {MODEL_YAML}")
    print()

    result = sync_model()
    model = result["model"]

    print(f"Schema: format={model['schema'].get('format')}")
    print(f"Anchors: {len(model['anchors'])}")
    print(f"Ties: {len(model['ties'])}")

    print()
    print(f"Written to {MODEL_YAML}")
    print(f"Sources written back to {MODEL_XML} <description> elements")

    # Warn about missing sources
    if result["missing_anchor_sources"] or result["missing_tie_sources"]:
        print()
        print("WARNING: Missing sources (add to model.yaml and re-sync):")
        if result["missing_anchor_sources"]:
            print(f"  Anchors: {sorted(result['missing_anchor_sources'])}")
        if result["missing_tie_sources"]:
            print(f"  Ties: {sorted(result['missing_tie_sources'])}")


if __name__ == "__main__":
    main()

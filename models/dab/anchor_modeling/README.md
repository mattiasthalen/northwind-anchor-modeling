# Anchor Modeling for SQLMesh

A complete implementation of [Anchor Modeling](https://en.wikipedia.org/wiki/Anchor_modeling) using SQLMesh Python blueprints with bidirectional XML/YAML synchronization.

## Overview

This package provides:

1. **SQLMesh Blueprint** - Generates all anchor model entities (anchors, ties, attributes, knots) from a unified YAML configuration
2. **Bidirectional Sync** - Keep Anchor Modeler XML and YAML source configurations in sync
3. **Incremental Loading** - All entities use an anti-join pattern for efficient incremental updates
4. **Comprehensive Tests** - 48 tests with 86% coverage

## Architecture

### Anchor Model Pattern

All anchor entities follow a consistent incremental pattern using an anti-join:

```sql
WITH target AS (
    SELECT unique_keys
    FROM existing_model
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_keys ORDER BY loaded_at DESC) = 1
),
source AS (
    -- Union all sources for this entity
    SELECT ...
)
SELECT columns
FROM source
LEFT ANTI JOIN target ON unique_keys
```

This ensures:
- Only new records are inserted
- Deduplication based on the most recent `loaded_at` timestamp
- Efficient incremental updates without full reprocessing

### Keyset ID Format

All anchor IDs use the keyset format:

```
{descriptor}@{system}[~{tenant}]|{key_values}
```

Examples:
- `Product@nw|42` - Product from northwind system, key 42
- `Product@nw~acme|42` - Product from northwind system, tenant acme, key 42
- `OrderDetail@nw|10248|11` - Composite key (order 10248, product 11)

## File Structure

```
models/dab/anchor_modeling/
├── README.md          # This file
├── blueprint.py       # SQLMesh model generation
├── sync.py            # XML ↔ YAML sync
├── model.xml          # Anchor Modeler file (design here)
├── model.yaml         # YAML config (edit sources here)
└── tests.py           # 48 tests, 86% coverage
```

## Workflow

### 1. Design the Model

Open `model.xml` in [Anchor Modeler](https://roenbaeck.github.io/anchor/) and design your model:
- Create anchors (entities)
- Create ties (relationships)
- Define attributes
- Create knots (reference data)

### 2. Sync to YAML

Run the sync to export the model structure and extract any existing source mappings:

```bash
uv run python -m models.dab.anchor_modeling.sync
```

This creates/updates `model.yaml` with the full model structure.

### 3. Configure Sources

Edit `model.yaml` to add source system mappings:

```yaml
anchors:
  PR:  # Product anchor
    descriptor: Product
    sources:
      - system: nw
        table: raw.northwind.products
        key: product_id
      - system: erp
        table: raw.erp.items
        key: item_id
        tenant: acme

ties:
  OR_order_PR_product:
    roles:
      - type: OR
        role: order
        identifier: true
      - type: PR
        role: product
        identifier: false
    sources:
      - system: nw
        table: raw.northwind.order_details
        keys:
          OR: order_id
          PR: product_id
```

### 4. Sync Back to XML

Run sync again to write sources back to XML `<description>` elements:

```bash
uv run python -m models.dab.anchor_modeling.sync
```

The sources are stored in XML so Anchor Modeler preserves them when you modify the model.

### 5. Generate SQLMesh Models

The blueprint automatically generates all SQLMesh models from `model.yaml`:

```bash
sqlmesh plan
```

This creates models like:
- `dab.anchor__PR` (Product anchor)
- `dab.anchor__OR` (Order anchor)
- `dab.tie__OR_order_PR_product` (Order-Product tie)

### 6. View Generated SQL

```bash
sqlmesh render '"dab.anchor__pr"'
sqlmesh render '"dab.tie__or_order_pr_product"'
```

## Source Configuration

### Anchor Sources

Anchors require:
- `system` - Source system identifier
- `table` - Source table name
- `key` - Column name(s) for the natural key
- `tenant` - (optional) Multi-tenant identifier

Single key:
```yaml
sources:
  - system: nw
    table: raw.products
    key: product_id
```

Composite key:
```yaml
sources:
  - system: nw
    table: raw.order_details
    key: [order_id, product_id]
```

### Tie Sources

Ties require:
- `system` - Source system identifier
- `table` - Source table name
- `keys` - Mapping of anchor roles to column names

```yaml
sources:
  - system: nw
    table: raw.order_details
    keys:
      OR: order_id
      PR: product_id
```

For self-referencing ties (same anchor type, different roles):
```yaml
sources:
  - system: hr
    table: raw.reports_to
    keys:
      PE_manager: manager_id
      PE_employee: employee_id
```

## Development

### Running Tests

```bash
# Run all tests
uv run python -m pytest models/dab/anchor_modeling/tests.py -v

# Run with coverage
uv run python -m pytest models/dab/anchor_modeling/tests.py --cov=models.dab.anchor_modeling --cov-report=term-missing
```

Current coverage: **86%** (48 tests)

### Adding New Entities

1. Update `model.xml` in Anchor Modeler
2. Run sync: `uv run python -m models.dab.anchor_modeling.sync`
3. Edit sources in `model.yaml`
4. Run sync again to write back to XML
5. Test with: `sqlmesh plan --dry-run`

## API Reference

### blueprint.py

Core SQL generation functions:

- `_build_keyset_expression(descriptor, system, key, tenant)` - Build keyset ID
- `_build_incremental_query(source_query, model_name, unique_keys, loaded_at_col, output_columns)` - Anti-join pattern
- `_build_anchor_query(blueprint, execution_ts, model_name)` - Generate anchor SQL
- `_build_tie_query(blueprint, execution_ts, model_name)` - Generate tie SQL
- `_get_blueprints()` - Load and validate model.yaml, return all blueprints

### sync.py

XML ↔ YAML synchronization:

- `sync_model(xml_path, yaml_path)` - Bidirectional sync
- `parse_xml_full(path)` - Parse XML to dict
- `write_sources_to_xml(xml_path, model)` - Write sources back to XML
- `build_tie_name(roles)` - Generate canonical tie name
- `to_snake_case(name)` - Convert PascalCase to snake_case

## Model Validation

The blueprint validates on load:

```python
class ModelValidationError(Exception):
    """Raised when model.yaml is invalid or incomplete."""
```

Validation checks:
- All anchors have sources defined
- Anchor sources have required fields: `system`, `table`, `key`
- All ties have sources defined
- Tie sources have required fields: `system`, `table`, `keys`

## SQLMesh Integration

The blueprint is a single SQLMesh Python model that generates multiple physical models using the `blueprints` parameter:

```python
@model(
    "@{model_name}",
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
```

Each blueprint creates a separate model:
- `model_name` - Full qualified name (e.g., `dab.anchor__pr`)
- `entity_type` - Either `anchor` or `tie`
- `sources` - List of source system mappings
- Additional entity-specific configuration

## Troubleshooting

### Model names are case-sensitive in CLI

SQLMesh lowercases model names internally. Always quote model names:

```bash
sqlmesh render '"dab.anchor__pr"'  # Correct
sqlmesh render dab.anchor__pr      # May fail
```

### Sync overwrites my manual XML edits

The sync is designed to be run after editing `model.yaml`. If you manually edit XML `<description>` elements, run sync to import those changes to YAML first.

### Validation errors on sqlmesh plan

Check that:
1. All anchors in `model.yaml` have sources defined
2. All ties have sources with the `keys` field
3. Required fields are present: `system`, `table`, `key`/`keys`

Run validation directly:
```python
from models.dab.anchor_modeling import blueprint
blueprint._get_blueprints()  # Raises ModelValidationError if invalid
```

## References

- [Anchor Modeling](https://en.wikipedia.org/wiki/Anchor_modeling) - Modeling technique
- [Anchor Modeler](https://roenbaeck.github.io/anchor/) - Visual modeling tool
- [SQLMesh Blueprints](https://sqlmesh.readthedocs.io/en/stable/concepts/models/python_models/#blueprints) - Multi-model generation
- [SQLMesh Incremental](https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/#incremental_unmanaged) - Unmanaged incremental loading

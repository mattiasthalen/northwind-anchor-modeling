"""
Anchor Models - Generated via Python Blueprint
"""

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

from macros.anchor_blueprint import get_anchor_blueprints, build_anchor_query, to_snake_case

# Get blueprint configs at module load (just metadata, no sqlglot objects)
_blueprint_data = get_anchor_blueprints()

# Extract mnemonic/descriptor/name for the decorator
_blueprints_for_model = [
    {
        "mnemonic": bp["mnemonic"],
        "descriptor": bp["descriptor"],
        "name": to_snake_case(bp["descriptor"]),  # e.g., "OrderDetails" → "order_details"
    }
    for bp in _blueprint_data
]

# Store source configs (not sqlglot objects) for runtime query generation
_sources = {bp["mnemonic"]: bp["sources"] for bp in _blueprint_data}

@model(
    "dab.anchor__@{name}",
    is_sql=True,
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": "@{mnemonic}_ID",
        "when_matched": "WHEN MATCHED THEN DO NOTHING",
    },
    blueprints=_blueprints_for_model,
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """Generate anchor query at runtime to avoid serialization issues."""
    mnemonic = evaluator.blueprint_var("mnemonic")
    sources = _sources[mnemonic]   
    execution_ts = evaluator.locals["execution_tstz"]
    query = build_anchor_query(mnemonic, sources, execution_ts)

    return query
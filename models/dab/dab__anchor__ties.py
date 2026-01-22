"""
Tie Models - Generated via Python Blueprint

Ties represent relationships between anchors.
"""

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

from macros.anchor_blueprint import get_tie_blueprints, build_tie_query, to_snake_case

# Get blueprint configs at module load
_blueprint_data = get_tie_blueprints()

# Extract tie_name and unique_key for the decorator
_blueprints_for_model = [
    {
        "tie_name": bp["tie_name"],
        "name": to_snake_case(bp["tie_name"]),
        "unique_key": bp["unique_key"],
    }
    for bp in _blueprint_data
]

# Store roles and sources for runtime query generation
_tie_configs = {
    bp["tie_name"]: {"roles": bp["roles"], "sources": bp["sources"]}
    for bp in _blueprint_data
}


@model(
    "dab.tie__@{name}",
    is_sql=True,
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": "@{unique_key}",
        "when_matched": "WHEN MATCHED THEN DO NOTHING",
    },
    blueprints=_blueprints_for_model,
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """Generate tie query at runtime to avoid serialization issues."""
    tie_name = evaluator.blueprint_var("tie_name")
    config = _tie_configs[tie_name]
    execution_ts = evaluator.locals["execution_tstz"]
    return build_tie_query(tie_name, config["roles"], config["sources"], execution_ts)

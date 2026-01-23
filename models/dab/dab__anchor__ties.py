"""
Tie Models - Generated via Python Blueprint

Ties represent relationships between anchors.
"""

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

from macros.anchor_blueprint import get_tie_blueprints, build_tie_query

# Get blueprint configs at module load
_blueprint_data = get_tie_blueprints()

# Store roles, sources, anchor descriptors, and columns for runtime
_tie_configs = {
    bp["tie_name"]: {
        "roles": bp["roles"],
        "sources": bp["sources"],
        "anchor_descriptors": bp["anchor_descriptors"],
        "columns": bp["columns"],
    }
    for bp in _blueprint_data
}

@model(
    "dab.tie__@{tie_name}",
    is_sql=True,
    kind={"name": ModelKindName.INCREMENTAL_UNMANAGED},
    blueprints=_blueprint_data,
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """Generate tie query at runtime to avoid serialization issues."""
    tie_name = evaluator.blueprint_var("tie_name")
    config = _tie_configs[tie_name]
    execution_ts = evaluator.locals["execution_tstz"]
    model_name = f"dab.tie__{tie_name}"
    return build_tie_query(
        tie_name,
        config["roles"],
        config["sources"],
        config["anchor_descriptors"],
        execution_ts,
        model_name,
    )

"""
Anchor Models - Generated via Python Blueprint
"""

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

from macros.anchor_blueprint import get_anchor_blueprints, build_anchor_query

# Get blueprint configs at module load
_blueprint_data = get_anchor_blueprints()

# Store source configs for runtime query generation
_configs = {
    bp["mnemonic"]: {"descriptor": bp["descriptor"], "sources": bp["sources"]}
    for bp in _blueprint_data
}

@model(
    "dab.anchor__@{mnemonic}",
    is_sql=True,
    kind={"name": ModelKindName.INCREMENTAL_UNMANAGED},
    blueprints=_blueprint_data,
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    """Generate anchor query at runtime to avoid serialization issues."""
    mnemonic = evaluator.blueprint_var("mnemonic")
    config = _configs[mnemonic]
    execution_ts = evaluator.locals["execution_tstz"]
    model_name = f"dab.anchor__{mnemonic}"
    return build_anchor_query(mnemonic, config["descriptor"], config["sources"], execution_ts, model_name)
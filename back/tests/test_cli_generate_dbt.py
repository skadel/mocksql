"""Regression tests for dbt resolution in ``mocksql generate``."""

import json
from pathlib import Path

from cli.generate import resolve_model_sql
from storage.config import get_dbt_project


def test_external_config_resolves_dbt_and_selects_compiled_sql(
    tmp_path: Path, monkeypatch
):
    """``--config`` wins over the CWD config when resolving a dbt model."""
    config_root = tmp_path / "external-config"
    models = config_root / "models" / "agg"
    models.mkdir(parents=True)
    model = models / "monthly_agg_reviews.sql"
    model.write_text("select {{ ref('raw_reviews') }}", encoding="utf-8")
    config = config_root / "mocksql.yml"

    dbt_root = tmp_path / "dbt-project"
    target = dbt_root / "target"
    compiled = target / "compiled" / "airbnb" / "models" / "agg"
    compiled.mkdir(parents=True)
    (target / "manifest.json").write_text(
        json.dumps(
            {
                "nodes": {
                    "model.airbnb.monthly_agg_reviews": {
                        "resource_type": "model",
                        "name": "monthly_agg_reviews",
                        "package_name": "airbnb",
                        "original_file_path": "models/agg/monthly_agg_reviews.sql",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (compiled / "monthly_agg_reviews.sql").write_text(
        "SELECT review_id FROM raw.reviews", encoding="utf-8"
    )
    config.write_text(
        "models_path: ./models\n"
        "dialect: bigquery\n"
        "dbt:\n"
        "  project_dir: ../dbt-project\n",
        encoding="utf-8",
    )

    # A conflicting CWD config models the original bug: it must be ignored.
    cwd = tmp_path / "unrelated-cwd"
    cwd.mkdir()
    (cwd / "mocksql.yml").write_text("dbt: {}\n", encoding="utf-8")
    monkeypatch.chdir(cwd)

    project = get_dbt_project(config)
    assert project is not None
    assert project.is_dbt_model("agg/monthly_agg_reviews")

    model_name, selected_sql, is_dbt_model = resolve_model_sql(
        model,
        config,
        {"models_path": "./models", "dialect": "bigquery"},
        project,
    )
    assert model_name == "agg/monthly_agg_reviews"
    assert is_dbt_model is True
    assert "review_id" in selected_sql
    assert "raw.reviews" in selected_sql
    assert "{{" not in selected_sql

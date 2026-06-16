"""Régression : `mocksql test` rejoue le SQL du DISQUE par défaut, pas le snapshot
figé dans le JSON. Sans ça, après qu'un agent édite le `.sql` source, `test`
testerait toujours l'ancien SQL et la boucle de fix ne verrait jamais le correctif.

- défaut          → lit `models_path/{model}.sql` (source = "disk")
- `--frozen`      → force le snapshot du JSON (source = "frozen")
- source absente  → fallback snapshot + warning (source = "snapshot-fallback"),
                    jamais un crash (suites portables type examples/spider).
"""

from cli.test_runner import resolve_run_sql


def _cfg(models_path):
    return {"models_path": str(models_path), "dialect": "bigquery"}


def test_frozen_returns_snapshot(tmp_path):
    sql, source = resolve_run_sql(
        cfg={"models_path": "models"},
        config_path=tmp_path / "mocksql.yml",
        model_name="orders",
        snapshot_sql="SELECT 'frozen' AS v",
        frozen=True,
    )
    assert source == "frozen"
    assert sql == "SELECT 'frozen' AS v"


def test_default_reads_disk(tmp_path):
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.sql").write_text("SELECT 1 AS x", encoding="utf-8")

    sql, source = resolve_run_sql(
        cfg=_cfg("models"),
        config_path=tmp_path / "mocksql.yml",
        model_name="orders",
        snapshot_sql="SELECT 'stale' AS v",
        frozen=False,
    )
    assert source == "disk"
    assert "1" in sql and "stale" not in sql


def test_default_reads_disk_nested_model(tmp_path):
    models = tmp_path / "models"
    (models / "demo").mkdir(parents=True)
    (models / "demo" / "payment_summary.sql").write_text(
        "SELECT 2 AS y", encoding="utf-8"
    )

    sql, source = resolve_run_sql(
        cfg=_cfg("models"),
        config_path=tmp_path / "mocksql.yml",
        model_name="demo/payment_summary",
        snapshot_sql="SELECT 'stale' AS v",
        frozen=False,
    )
    assert source == "disk"
    assert "2" in sql


def test_missing_source_falls_back_to_snapshot(tmp_path):
    (tmp_path / "models").mkdir()
    sql, source = resolve_run_sql(
        cfg=_cfg("models"),
        config_path=tmp_path / "mocksql.yml",
        model_name="ghost",
        snapshot_sql="SELECT 'snap' AS v",
        frozen=False,
    )
    assert source == "snapshot-fallback"
    assert sql == "SELECT 'snap' AS v"

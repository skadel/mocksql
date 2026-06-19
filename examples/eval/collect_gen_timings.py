"""Collecte des durées de génération pour l'estimateur grosse-maille.

Lance `mocksql generate --overwrite` sur chaque modèle d'un sous-projet : chaque
génération « pleine » appende automatiquement une ligne
`(sql_len, n_tables, n_used_cols, n_constraints, elapsed_sec)` au dataset
`back/build_query/data/gen_timings.jsonl` (via l'instrumentation de run_generate).

Aucune logique de timing ici — on se contente de DÉCLENCHER des générations
réelles ; l'estimation s'affine ensuite toute seule via estimate_minutes().

Prérequis : credentials Vertex/GCP (cf. skill eval-mocksql).

Usage :
    poetry -C back run python examples/eval/collect_gen_timings.py [projet] [--limit N]

    projet  : chemin du sous-projet (défaut examples/spider)
    --limit : nombre max de modèles à générer (défaut : tous)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACK = ROOT / "back"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", nargs="?", default=str(ROOT / "examples" / "spider"))
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    project = Path(args.project).resolve()
    models = sorted((project / "models").glob("*.sql"))
    if args.limit:
        models = models[: args.limit]
    if not models:
        print(f"Aucun modèle .sql trouvé dans {project / 'models'}", file=sys.stderr)
        return 1

    config = project / "mocksql.yml"
    output = project / ".mocksql" / "tests"
    env = {
        **os.environ,
        "DUCKDB_PATH": str(project / ".mocksql" / "mocksql.duckdb"),
    }

    print(f"Collecte sur {len(models)} modèle(s) de {project.name}…")
    ok = 0
    for i, model in enumerate(models, 1):
        print(f"[{i}/{len(models)}] {model.stem}…", flush=True)
        result = subprocess.run(
            [
                "poetry", "-C", str(BACK), "run", "mocksql", "generate",
                str(model),
                "--config", str(config),
                "--output", str(output),
                "--overwrite",
            ],
            env=env,
        )
        ok += result.returncode == 0

    print(f"\nTerminé : {ok}/{len(models)} générations réussies.")
    print("Dataset : back/build_query/data/gen_timings.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

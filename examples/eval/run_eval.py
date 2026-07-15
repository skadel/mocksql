"""
Eval pipeline MockSQL — évalue la qualité des premiers tests générés.

Usage (depuis examples/eval/) :
  python run_eval.py --project ../spider
  python run_eval.py --project ../spider --models bq282 bq199
  python run_eval.py --project ../spider --out results/2026-05-16_spider.json
  python run_eval.py --project ../spider --gcp-project my-gcp-project
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

_BACK_ENV = Path(__file__).parent.parent.parent / "back" / ".env"
load_dotenv(_BACK_ENV)

sys.path.insert(0, str(Path(__file__).parent))
from judge import judge_first_test

# L'import de judge a mis back/ dans sys.path — les modules storage sont accessibles.
from storage.test_files import read_test_doc  # noqa: E402


def load_test_file(path: Path) -> dict | None:
    """Lit le fichier de tests via la fonction de lecture OFFICIELLE, qui fusionne le
    sidecar gitignoré `.mocksql/cache/{model}.json` (status, results_json…).

    Un `json.loads` brut du seul fichier commité privait le juge du résultat réel
    (`results_json`) et du `status` → 13 faux « KO — résultat vide » sur l'éval
    spider2-snow (~12 points de pass-rate perdus par artefact de harnais).
    """
    return read_test_doc(path)


def collect_models(tests_dir: Path, filter_names: list[str] | None) -> list[Path]:
    if not tests_dir.exists():
        print(f"[error] Dossier tests introuvable : {tests_dir}", file=sys.stderr)
        sys.exit(1)
    files = sorted(tests_dir.rglob("*.json"))
    if filter_names:
        files = [f for f in files if f.stem in filter_names]
    return files


def run_eval(
    project_dir: Path,
    filter_models: list[str] | None,
    gcp_project: str | None,
    out_path: Path,
    gemini_model: str | None = None,
) -> None:
    tests_dir = project_dir / ".mocksql" / "tests"
    test_files = collect_models(tests_dir, filter_models)

    if not test_files:
        print("[warn] Aucun fichier de tests trouvé. Générez d'abord des tests via l'UI MockSQL.")
        return

    results = []
    n_valid = 0

    for tf in test_files:
        data = load_test_file(tf)
        if not data:
            print(f"[skip] {tf.stem} — fichier illisible")
            continue

        test_cases = data.get("test_cases", [])
        if not test_cases:
            print(f"[skip] {tf.stem} — aucun test_case")
            results.append({"name": tf.stem, "skipped": True, "reason": "no_test_cases"})
            continue

        first_test = test_cases[0]
        sql = data.get("sql") or data.get("optimized_sql", "")
        if not sql:
            print(f"[skip] {tf.stem} — SQL absent")
            results.append({"name": tf.stem, "skipped": True, "reason": "no_sql"})
            continue

        print(f"[eval] {tf.stem} ...", end=" ", flush=True)
        judgment = judge_first_test(
            sql=sql,
            test_case=first_test,
            gcp_project=gcp_project,
            model_name=gemini_model,
        )

        exec_status = (
            first_test.get("exec_status")
            or first_test.get("status")
            or "unknown"
        )
        is_valid = judgment.get("is_valid", False)
        if is_valid:
            n_valid += 1

        print(
            f"donnees={judgment.get('cohérence_données', '?')} "
            f"test={judgment.get('cohérence_test', '?')} "
            f"lisibilite={judgment.get('lisibilité_métier', '?')} "
            f"{'OK' if is_valid else 'KO'}"
        )

        results.append(
            {
                "name": tf.stem,
                "exec_status": exec_status,
                "cohérence_données": judgment.get("cohérence_données"),
                "cohérence_test": judgment.get("cohérence_test"),
                "lisibilité_métier": judgment.get("lisibilité_métier"),
                "reasoning": judgment.get("reasoning", ""),
                "is_valid": is_valid,
            }
        )

    n_total = sum(1 for r in results if not r.get("skipped"))
    pass_rate = round(n_valid / n_total, 3) if n_total else 0

    report = {
        "meta": {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "project": project_dir.name,
            "gemini_model": gemini_model or os.environ.get("DEFAULT_MODEL_NAME", "auto"),
            "n_models": n_total,
            "n_valid": n_valid,
            "pass_rate": pass_rate,
        },
        "models": results,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n--- {n_valid}/{n_total} valides ({pass_rate:.0%}) ---")
    print(f"Rapport sauvegardé : {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Eval LLM-as-judge pour MockSQL")
    parser.add_argument("--project", required=True, help="Chemin vers le sous-projet (ex: ../spider)")
    parser.add_argument("--models", nargs="*", help="Subset de modèles (ex: bq282 bq199)")
    parser.add_argument("--out", help="Chemin du rapport JSON de sortie")
    parser.add_argument("--gcp-project", help="GCP project ID pour VertexAI")
    parser.add_argument("--gemini-model", default=None, help="Modèle à utiliser (défaut : DEFAULT_MODEL_NAME ou config mocksql)")
    args = parser.parse_args()

    project_dir = Path(args.project).resolve()
    if not project_dir.exists():
        print(f"[error] Projet introuvable : {project_dir}", file=sys.stderr)
        sys.exit(1)

    date_str = datetime.now().strftime("%Y-%m-%d")
    default_out = Path(__file__).parent / "results" / f"{date_str}_{project_dir.name}.json"
    out_path = Path(args.out) if args.out else default_out

    gcp_project = args.gcp_project or os.environ.get("GOOGLE_CLOUD_PROJECT")

    run_eval(
        project_dir=project_dir,
        filter_models=args.models,
        gcp_project=gcp_project,
        out_path=out_path,
        gemini_model=args.gemini_model,
    )


if __name__ == "__main__":
    main()

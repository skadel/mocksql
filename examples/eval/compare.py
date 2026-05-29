"""
Compare deux rapports d'eval MockSQL pour détecter les régressions.

Usage :
  python compare.py results/before.json results/after.json
  python compare.py results/before.json results/after.json --threshold 0.5
"""

import argparse
import json
import sys
from pathlib import Path


def load_report(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[error] Impossible de lire {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def score(model_result: dict) -> float:
    values = [
        model_result.get("cohérence_données") or 0,
        model_result.get("cohérence_test") or 0,
        model_result.get("lisibilité_métier") or 0,
    ]
    non_zero = [v for v in values if v > 0]
    return round(sum(non_zero) / len(non_zero), 2) if non_zero else 0


def compare(before_path: Path, after_path: Path, threshold: float) -> None:
    before = load_report(before_path)
    after = load_report(after_path)

    before_by_name = {m["name"]: m for m in before.get("models", []) if not m.get("skipped")}
    after_by_name = {m["name"]: m for m in after.get("models", []) if not m.get("skipped")}

    all_names = sorted(set(before_by_name) | set(after_by_name))

    regressions = []
    improvements = []
    unchanged = []

    for name in all_names:
        b = before_by_name.get(name)
        a = after_by_name.get(name)

        if b is None:
            print(f"  {name:<20}  nouveau")
            continue
        if a is None:
            print(f"  {name:<20}  supprimé")
            continue

        b_score = score(b)
        a_score = score(a)
        delta = round(a_score - b_score, 2)

        b_valid = "✓" if b.get("is_valid") else "✗"
        a_valid = "✓" if a.get("is_valid") else "✗"
        valid_change = f"{b_valid} → {a_valid}"

        if delta <= -threshold:
            tag = f"⚠ régression ({delta:+.2f})"
            regressions.append(name)
        elif delta >= threshold:
            tag = f"↑ amélioration ({delta:+.2f})"
            improvements.append(name)
        else:
            tag = f"= stable ({delta:+.2f})"
            unchanged.append(name)

        print(f"  {name:<20}  {valid_change}  score: {b_score} → {a_score}  {tag}")

    before_meta = before.get("meta", {})
    after_meta = after.get("meta", {})

    print()
    print(f"Pass rate : {before_meta.get('pass_rate', '?'):.0%} → {after_meta.get('pass_rate', '?'):.0%}")
    print(f"Régressions : {len(regressions)}  |  Améliorations : {len(improvements)}  |  Stables : {len(unchanged)}")

    if regressions:
        print(f"\nModèles régressés : {', '.join(regressions)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare deux rapports d'eval MockSQL")
    parser.add_argument("before", help="Rapport avant (JSON)")
    parser.add_argument("after", help="Rapport après (JSON)")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Delta minimum pour signaler une régression/amélioration (défaut: 0.5)",
    )
    args = parser.parse_args()

    compare(Path(args.before), Path(args.after), args.threshold)


if __name__ == "__main__":
    main()

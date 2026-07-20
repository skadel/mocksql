"""mocksql export dbt — compile les tests MockSQL en unit tests dbt natifs (≥ 1.8).

Spec ``docs/spec-export-dbt-unit-tests.md`` + ``docs/spec-validation-humaine.md`` §7.

Décision structurante (validation-humaine) : le **contrat ``expect``** (lignes attendues,
confirmées par un humain) EST le bloc ``expect:`` d'un unit test dbt — l'export est une
transformation quasi triviale, **déterministe, zéro LLM, sans replay** (``expect`` est
commité dans la définition, disponible sur un clone). Contrat unique replay ↔ export.

Gates (chaque exclusion listée avec sa raison, jamais silencieuse) :
- niveau cas : contrat ``expect`` présent ; ``review.status == "confirmed"`` (seul un
  contrat signé par un humain doit gater la CI de l'utilisateur) ; pas mort-né ; toutes
  les tables de ``data`` résolues en ``ref``/``source`` ; valeurs sérialisables en dict YAML.
- niveau modèle : nœud dbt résolu ; matérialisation ∉ {incremental, materialized_view}.

Rendu déterministe (tri par ``test_uid``, ordre de clés fixe) → re-export sans changement
= fichier identique.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from cli.test_runner import _flatten_table_key
from storage.test_files import is_deadborn_case

_HEADER = (
    "# Généré par MockSQL — NE PAS ÉDITER À LA MAIN.\n"
    "# Régénéré par `mocksql export dbt`. Les lignes proviennent du contrat `expect`\n"
    "# (sortie confirmée par un humain). Édite le test côté MockSQL puis ré-exporte.\n"
)

# Types représentables tels quels dans une fixture dict dbt (YAML scalaire).
_SCALAR = (str, int, float, bool)

# Matérialisations hors périmètre v1 (sémantique d'expect différente / non testable en unit).
_UNSUPPORTED_MATERIALIZATIONS = ("incremental", "materialized_view", "ephemeral")


def _slug(name: str) -> str:
    """``Test name éàî`` → ``test_name_eai`` : ascii minuscule, non-alnum → ``_``."""
    ascii_name = (
        unicodedata.normalize("NFKD", name or "")
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")
    return slug or "test"


def _uid8(test_uid: Optional[str]) -> str:
    """8 premiers hex du ``test_uid`` (tirets retirés) → suffixe stable/unique par modèle."""
    hexed = re.sub(r"[^0-9a-fA-F]", "", str(test_uid or ""))
    return (hexed[:8] or "00000000").lower()


def _unserializable_value(rows: List[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    """Première ``(colonne, type)`` non représentable en dict YAML, ou None.

    Rejette VARIANT/JSON/STRUCT (dict/list) et BYTES en v1 (spec §5) — tout le reste des
    valeurs normalisées de ``results_json`` est scalaire (str/int/float/bool/None)."""
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for col, val in row.items():
            if val is None or isinstance(val, _SCALAR):
                continue
            return str(col), type(val).__name__
    return None


@dataclass
class ExportResult:
    model: str
    node_name: Optional[str] = None
    unit_tests: List[Dict[str, Any]] = field(default_factory=list)
    excluded: List[Tuple[str, str]] = field(default_factory=list)  # (test id, raison)
    model_error: Optional[str] = None


def _build_parent_index(project, node) -> Dict[str, str]:
    """``{clé de table aplatie: jinja ref()/source()}`` pour tous les parents du modèle."""
    return {
        _flatten_table_key(dotted): jinja
        for dotted, jinja in project.parent_relations(node).items()
    }


def _case_id(case: Dict[str, Any]) -> str:
    return str(case.get("test_uid") or case.get("test_index") or "?")


def export_case(
    case: Dict[str, Any], node_name: str, parent_index: Dict[str, str]
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Compile UN cas en unit test dbt, ou ``(None, raison d'exclusion)``.

    ``given`` liste TOUS les parents du DAG (dbt l'exige) : les tables peuplées par le cas
    reçoivent leurs lignes, les autres une fixture vide (``rows: []``). ``expect`` = lignes
    du contrat restreintes à ``expect.columns``.
    """
    if is_deadborn_case(case):
        return None, "mort-né (exécution jamais réussie)"

    review = case.get("review") if isinstance(case.get("review"), dict) else {}
    status = review.get("status")
    if status != "confirmed":
        label = {"draft": "à confirmer", "stale": "SQL modifié — à re-confirmer"}.get(
            status or "", "non confirmé"
        )
        return None, f"non confirmé ({label})"

    expect = case.get("expect") if isinstance(case.get("expect"), dict) else None
    if expect is None:
        return None, "pas de contrat expect (relance puis confirme le test)"

    data: Dict[str, Any] = case.get("data") or {}
    # Résolution des tables peuplées → parents dbt (suffixe-matching via clé aplatie).
    rows_by_parent: Dict[str, List[Dict[str, Any]]] = {}
    for tname, rows in data.items():
        fk = _flatten_table_key(tname)
        if fk not in parent_index:
            return None, f"table hors DAG dbt : {tname}"
        rows_by_parent[fk] = rows if isinstance(rows, list) else []

    # Sérialisabilité : given (données d'entrée) + expect (sortie).
    for rows in list(rows_by_parent.values()) + [expect.get("rows") or []]:
        bad = _unserializable_value(rows)
        if bad:
            return None, f"valeur non sérialisable en dict YAML : {bad[0]} ({bad[1]})"

    # given : tous les parents, ordre déterministe (par jinja), fixture vide si non peuplé.
    given = [
        {"input": jinja, "rows": rows_by_parent.get(fk, [])}
        for fk, jinja in sorted(parent_index.items(), key=lambda kv: kv[1])
    ]

    name = (
        f"mocksql__{_slug(case.get('test_name') or '')}__{_uid8(case.get('test_uid'))}"
    )
    unit_test: Dict[str, Any] = {
        "name": name,
        "model": node_name,
        "given": given,
        "expect": {"rows": list(expect.get("rows") or [])},
        "config": {"tags": ["mocksql"]},
    }
    description = (case.get("unit_test_description") or "").strip()
    if description:
        unit_test["description"] = description
    return unit_test, None


def export_doc(doc: Dict[str, Any], model_name: str, project) -> ExportResult:
    """Compile un doc de test complet. Cas triés par ``test_uid`` (rendu déterministe)."""
    node = project.find_node(model_name)
    if node is None:
        return ExportResult(
            model=model_name,
            model_error="pas un modèle dbt (introuvable dans le manifest)",
        )

    materialization = (node.get("config") or {}).get("materialized", "table")
    if materialization in _UNSUPPORTED_MATERIALIZATIONS:
        return ExportResult(
            model=model_name,
            node_name=node.get("name"),
            model_error=f"matérialisation hors périmètre v1 : {materialization}",
        )

    parent_index = _build_parent_index(project, node)
    result = ExportResult(model=model_name, node_name=node.get("name"))
    cases = sorted(
        doc.get("test_cases") or [], key=lambda c: str(c.get("test_uid") or "")
    )
    for case in cases:
        unit_test, reason = export_case(case, node.get("name"), parent_index)
        if unit_test is not None:
            result.unit_tests.append(unit_test)
        else:
            result.excluded.append((_case_id(case), reason or "exclu"))
    return result


class _BlockDumper(yaml.SafeDumper):
    """Dumper YAML : indentation de bloc lisible (listes indentées sous leur clé)."""


def _str_presenter(dumper: yaml.Dumper, data: str):
    # `ref('x')` / `source('a','b')` restent des scalaires plats (pas de guillemets
    # parasites) tant qu'ils ne contiennent pas de caractère YAML dangereux.
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_BlockDumper.add_representer(str, _str_presenter)
_BlockDumper.ignore_aliases = lambda self, data: True  # jamais d'ancres YAML (&id001)


def render_yaml(unit_tests: List[Dict[str, Any]]) -> str:
    """Rend le YAML dbt final (en-tête + ``unit_tests:``). Déterministe (ordre préservé)."""
    body = yaml.dump(
        {"unit_tests": unit_tests},
        Dumper=_BlockDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
        indent=2,
    )
    return _HEADER + body


def _output_path(models_base: Path, model_name: str) -> Path:
    """``{dir du .sql}/{model}.mocksql.yml`` — le YAML vit à côté du modèle source."""
    return models_base / f"{model_name}.mocksql.yml"


class ExportError(Exception):
    """Erreur de configuration/résolution rendant l'export impossible (exit 1)."""


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


def _load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _resolve_project(cfg: dict, config_path: Path):
    dbt_cfg = cfg.get("dbt")
    if not dbt_cfg or not dbt_cfg.get("project_dir"):
        raise ExportError(
            "Aucun projet dbt configuré. Ajoute un bloc `dbt: {project_dir: ...}` à "
            "mocksql.yml (cf. spec export dbt §3)."
        )
    from storage.dbt_manifest import DbtProject

    project_dir = (config_path.parent / dbt_cfg["project_dir"]).resolve()
    return DbtProject(project_dir, dbt_cfg.get("target_path", "target"))


def _discover_models(tests_root: Path, project) -> List[str]:
    """Tous les modèles de ``.mocksql/tests`` qui résolvent vers un nœud dbt (``--all``)."""
    if not tests_root.exists():
        return []
    models: List[str] = []
    for f in sorted(tests_root.rglob("*.json")):
        if _UUID_RE.match(f.stem):
            continue
        model_name = f.relative_to(tests_root).with_suffix("").as_posix()
        if project.find_node(model_name) is not None:
            models.append(model_name)
    return models


@dataclass
class ModelExport:
    result: ExportResult
    rendered: Optional[str] = None
    path: Optional[Path] = None
    action: str = "none"  # written | unchanged | drift | dry-run | skipped | error


def run_export(
    config_path: Path,
    targets: Optional[List[str]] = None,
    all_models: bool = False,
    check: bool = False,
    dry_run: bool = False,
) -> Tuple[int, List[ModelExport]]:
    """Exporte les tests MockSQL en YAML dbt. Déterministe, zéro LLM (lit ``expect``).

    Retourne ``(exit_code, exports)`` — exit 1 si dérive (``--check``), erreur de
    résolution, ou aucun cas exportable au total.
    """
    from storage.test_files import read_test_doc

    cfg = _load_config(config_path)
    project = _resolve_project(cfg, config_path)
    models_base = (config_path.parent / cfg.get("models_path", "models")).resolve()
    tests_root = config_path.parent / ".mocksql" / "tests"

    if targets:
        model_names = list(targets)
    elif all_models:
        model_names = _discover_models(tests_root, project)
    else:
        raise ExportError("Précise au moins -t <model> ou --all.")

    exports: List[ModelExport] = []
    exit_code = 0
    total_unit_tests = 0

    for model_name in model_names:
        test_file = tests_root / f"{model_name}.json"
        doc = read_test_doc(test_file)
        if not doc:
            exports.append(
                ModelExport(
                    result=ExportResult(
                        model=model_name, model_error="aucun test sauvegardé"
                    ),
                    action="skipped",
                )
            )
            continue

        result = export_doc(doc, model_name, project)
        if result.model_error or not result.unit_tests:
            exports.append(
                ModelExport(
                    result=result,
                    action="skipped" if not result.model_error else "error",
                )
            )
            continue

        total_unit_tests += len(result.unit_tests)
        rendered = render_yaml(result.unit_tests)
        out_path = _output_path(models_base, model_name)

        if dry_run:
            exports.append(
                ModelExport(
                    result=result, rendered=rendered, path=out_path, action="dry-run"
                )
            )
            continue

        current = out_path.read_text(encoding="utf-8") if out_path.exists() else None
        if check:
            action = "unchanged" if current == rendered else "drift"
            if action == "drift":
                exit_code = 1
            exports.append(
                ModelExport(
                    result=result, rendered=rendered, path=out_path, action=action
                )
            )
            continue

        if current == rendered:
            action = "unchanged"
        else:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(rendered, encoding="utf-8")
            action = "written"
        exports.append(
            ModelExport(result=result, rendered=rendered, path=out_path, action=action)
        )

    # Aucun cas exportable au total (et pas déjà en dérive) → exit 1 (spec §3).
    if total_unit_tests == 0 and not check:
        exit_code = 1
    return exit_code, exports

from typing import List

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from build_query.integration_runner import (
    list_integration_files,
    load_integration_spec,
    run_integration_file,
    get_source_tables,
    validate_chain,
)
from storage.config import get_mocksql_dir, get_models_path

router = APIRouter()


class IntegrationStepSchema(BaseModel):
    sql: str
    produces: str


class SaveIntegrationRequest(BaseModel):
    filename: str
    name: str
    chain: List[IntegrationStepSchema]


class RunIntegrationRequest(BaseModel):
    file: str
    project: str
    dialect: str = "bigquery"


@router.post("/integration")
async def save_integration(body: SaveIntegrationRequest):
    """Crée ou met à jour un fichier d'intégration YAML dans .mocksql/integration/."""
    filename = (
        body.filename if body.filename.endswith(".yml") else f"{body.filename}.yml"
    )
    integration_dir = get_mocksql_dir() / "integration"
    integration_dir.mkdir(parents=True, exist_ok=True)
    path = integration_dir / filename
    spec = {
        "name": body.name,
        "chain": [{"sql": s.sql, "produces": s.produces} for s in body.chain],
        "tests": [],
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(
            spec, f, allow_unicode=True, default_flow_style=False, sort_keys=False
        )
    return {"filename": filename, "saved": True}


@router.get("/integration")
async def get_integration_files():
    """Liste les fichiers d'intégration dans .mocksql/integration/."""
    try:
        return {"files": list_integration_files()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/integration/{filename}/source_tables")
async def get_integration_source_tables(filename: str, dialect: str = "bigquery"):
    """
    Pour chaque étape de la chaîne, retourne les tables sources réelles à importer.
    Les tables intermédiaires déclarées dans 'produces' sont exclues.

    Utilisé pour alimenter l'ImportView avant la génération des tests.
    """
    try:
        spec = load_integration_spec(filename)
        chain = spec.get("chain", [])
        models_path = get_models_path()
        source_tables = get_source_tables(chain, dialect, models_path)
        return {
            "name": spec.get("name", filename),
            "chain": chain,
            "source_tables": source_tables,
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/integration/{filename}/validate")
async def validate_integration_chain(filename: str, dialect: str = "bigquery"):
    """
    Valide la syntaxe de chaque script de la chaîne via sqlglot.
    Retourne un verdict par étape (valid + error si applicable).
    """
    try:
        spec = load_integration_spec(filename)
        chain = spec.get("chain", [])
        models_path = get_models_path()
        results = validate_chain(chain, dialect, models_path)
        all_valid = all(r["valid"] for r in results)
        return {"all_valid": all_valid, "steps": results}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/integration/run")
async def run_integration(body: RunIntegrationRequest):
    """Exécute tous les tests d'un fichier d'intégration YAML."""
    try:
        result = await run_integration_file(
            filename=body.file,
            project=body.project,
            dialect=body.dialect,
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

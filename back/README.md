# MockSQL — Backend

> Pour l'installation, la configuration GCP et le CLI, voir le [README racine](../README.md).

**FastAPI · LangGraph · Python 3.12**

---

## Développement local

```bash
python -m venv .venv
source .venv/bin/activate   # Windows : .\.venv\Scripts\activate
pip install poetry && poetry install

cp .env.example .env        # compléter les variables (voir README racine §3)
uvicorn server:app --port 8080 --reload
```

---

## Commandes

```bash
make style    # ruff check + ruff format --check + vulture (code mort)
make format   # auto-format + auto-fix ruff
make test     # pytest
make check    # style + test
```

Type checking :

```bash
poetry run mypy build_query/ app/
```

---

## Packaging

MockSQL produit deux wheels :

| Wheel | Contenu |
|-------|---------|
| `mocksql-*.whl` | CLI + LangGraph core (sans UI) |
| `mocksql_ui-*.whl` | Serveur web + assets React bundlés |

```bash
make build-cli   # CLI uniquement
make build-ui    # CLI + UI (Node.js 18+ requis pour le build React)
```

Les wheels sont générés dans `dist/`.

---

## Licence

Propriétaire — © 2025 Adel Skhiri. Contact : [skhiriadel92@gmail.com](mailto:skhiriadel92@gmail.com)

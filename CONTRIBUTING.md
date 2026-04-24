# Contributing to MockSQL

Thank you for your interest in contributing!

## Prerequisites

- Python 3.12+ and [Poetry](https://python-poetry.org/)
- Node.js 18+ and npm
- Google Cloud SDK (for BigQuery features)

## Setup

```bash
# Backend
cd back
poetry install
cp .env.example .env   # fill in your values

# Frontend
cd front
npm ci
```

## Development workflow

```bash
# Backend — quality checks
make style     # ruff + vulture
make format    # auto-fix
make test      # pytest
make check     # style + test

# Frontend
npm test
npx eslint src/
```

## Pull requests

1. Fork the repo and create a branch from `master`.
2. Make your changes and ensure `make check` passes (backend) and `npm test` passes (frontend).
3. Open a PR — describe *what* changed and *why*.
4. A maintainer will review and merge.

## Releasing a new version (maintainers only)

Releases are fully automated via GitHub Actions. To cut a release:

```bash
# 1. Bump the version in back/pyproject.toml
cd back && poetry version patch   # or minor / major

# 2. Commit and tag
git add back/pyproject.toml
git commit -m "chore: release v$(cd back && poetry version -s)"
git tag "v$(cd back && poetry version -s)"
git push origin main --tags
```

The [release workflow](.github/workflows/release.yml) will then:
- Run all tests
- Build the Python wheel + sdist
- Publish to [PyPI](https://pypi.org/project/mocksql/)
- Build the frontend bundle
- Create a GitHub Release with all artifacts

### PyPI trusted publishing setup (one-time)

1. Create an account on [pypi.org](https://pypi.org) and reserve the project name `mocksql`.
2. Go to **Account settings → Publishing → Add a new pending publisher** and fill in:
   - Repository owner: your GitHub username / org
   - Repository name: `mocksql`
   - Workflow name: `release.yml`
   - Environment name: `pypi`
3. In your GitHub repo, create an environment named `pypi` (**Settings → Environments**).

No API token needed — PyPI and GitHub exchange credentials via OIDC.

## Reporting issues

Use the [issue templates](.github/ISSUE_TEMPLATE/) to report bugs or request features.

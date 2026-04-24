.PHONY: style format test check build-cli build-ui build-all

build-cli:
	poetry build --output dist

build-ui:
	cd ../front && npm ci && npm run build
	npx rimraf static ui/mocksql_ui/static
	npx cpx "../front/build/**" static
	npx cpx "../front/build/**" ui/mocksql_ui/static
	poetry build --output dist
	cd ui && poetry build --output ../dist

build-all: build-ui

style:
	poetry run ruff check .
	poetry run ruff format --check .
	poetry run vulture . --min-confidence 80 --exclude ".venv,venv"

format:
	poetry run ruff format .
	poetry run ruff check --fix .

test:
	poetry run pytest tests/

check: style test

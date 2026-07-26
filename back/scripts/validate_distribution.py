"""Install and smoke-test a built MockSQL distribution in isolated venvs.

This deliberately uses neither the source checkout nor PYTHONPATH.  It is used
by GitHub Actions after ``poetry build`` and can also validate a release
candidate locally on Windows.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path


def run(
    command: list[str], cwd: Path, *, capture_output: bool = False
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return subprocess.run(  # noqa: S603 -- commands are assembled from CI-controlled paths
        command,
        cwd=cwd,
        env=env,
        text=True,
        check=True,
        capture_output=capture_output,
    )


def venv_python(root: Path, name: str) -> Path:
    venv = root / name
    run([sys.executable, "-m", "venv", str(venv)], root)
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install(python: Path, wheel: Path, extra: str = "") -> None:
    run([str(python), "-m", "pip", "install", "--upgrade", "pip"], wheel.parent)
    requirement = f"mocksql{extra} @ {wheel.as_uri()}"
    run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            requirement,
        ],
        wheel.parent,
    )
    run([str(python), "-m", "pip", "check"], wheel.parent)


def candidate_wheel(dist: Path) -> tuple[Path, str]:
    wheels = list(dist.glob("mocksql-*.whl"))
    if not wheels:
        raise RuntimeError("no mocksql wheel found")
    wheel = max(wheels, key=lambda path: path.stat().st_mtime)
    with zipfile.ZipFile(wheel) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = BytesParser().parsebytes(archive.read(metadata_name))
    if metadata["Name"] != "mocksql" or not metadata["Version"]:
        raise RuntimeError("wheel has invalid mocksql metadata")
    return wheel, metadata["Version"]


def assert_wheel_assets(dist: Path, wheel: Path, version: str) -> None:
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    required = {
        "cli/main.py",
        "build_query/query_chain.py",
        "storage/config.py",
        "static/index.html",
        "static/manifest.json",
    }
    missing = required - names
    if missing:
        raise RuntimeError(f"wheel is missing required files: {sorted(missing)}")
    if not any(name.startswith("static/assets/") for name in names):
        raise RuntimeError("wheel is missing compiled frontend assets")
    sdists = list(dist.glob(f"mocksql-{version}.tar.gz"))
    if not sdists:
        raise RuntimeError("sdist is missing")
    with tarfile.open(sdists[0]) as archive:
        sdist_names = set(archive.getnames())
    sdist_required = {
        "pyproject.toml",
        "README.md",
        "cli/main.py",
        "build_query/query_chain.py",
        "static/index.html",
    }
    if not all(
        any(name.endswith(required) for name in sdist_names)
        for required in sdist_required
    ):
        raise RuntimeError("sdist is missing package sources")


def mocksql_command(python: Path) -> list[str]:
    executable = python.parent / ("mocksql.exe" if os.name == "nt" else "mocksql")
    return [str(executable)]


def assert_installed_candidate(
    python: Path, wheel: Path, version: str, cwd: Path
) -> None:
    probe = (
        "import importlib.metadata as m, json; "
        "d=m.distribution('mocksql'); "
        "print(json.dumps({'version': d.version, 'direct_url': "
        "d.read_text('direct_url.json')}))"
    )
    result = run([str(python), "-c", probe], cwd, capture_output=True)
    import json

    installed = json.loads(result.stdout)
    if installed["version"] != version or wheel.as_uri() not in installed["direct_url"]:
        raise RuntimeError(
            "installed mocksql metadata does not point to the candidate wheel"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, required=True)
    args = parser.parse_args()
    dist = args.dist.resolve()
    wheel, version = candidate_wheel(dist)
    assert_wheel_assets(dist, wheel, version)

    with tempfile.TemporaryDirectory(prefix="mocksql-dist-") as temp:
        root = Path(temp)
        project = root / "project"
        (project / "models").mkdir(parents=True)

        base = venv_python(root, "base")
        install(base, wheel)
        assert_installed_candidate(base, wheel, version, project)
        run(mocksql_command(base) + ["--help"], project)
        run(
            mocksql_command(base)
            + [
                "init",
                "--path",
                str(project),
                "--models-path",
                "./models",
                "--dialect",
                "duckdb",
                "--llm-provider",
                "openai",
                "--test-dataset",
                "test_dataset",
                "--non-interactive",
            ],
            project,
        )
        if not (project / "mocksql.yml").is_file():
            raise RuntimeError("mocksql init did not create mocksql.yml")

        for extra, modules in {
            "[bigquery]": ["google.cloud.bigquery"],
            "[snowflake]": ["snowflake.connector"],
            "[all]": ["google.cloud.bigquery", "snowflake.connector", "trino"],
        }.items():
            python = venv_python(root, extra[1:-1])
            install(python, wheel, extra)
            assert_installed_candidate(python, wheel, version, project)
            for module in modules:
                run([str(python), "-c", f"import {module}"], project)

        missing_connectors = {
            "bigquery": "from utils.optional_deps import import_bigquery; import_bigquery()",
            "snowflake": "from utils.snowflake_connector import _import_snowflake; _import_snowflake()",
            "trino": "from utils.optional_deps import import_trino; import_trino()",
        }
        for connector, probe in missing_connectors.items():
            missing = subprocess.run(  # noqa: S603 -- installed venv and fixed probe only
                [str(base), "-c", probe],
                cwd=project,
                text=True,
                capture_output=True,
                env={
                    key: value
                    for key, value in os.environ.items()
                    if key != "PYTHONPATH"
                },
            )
            expected = f"pip install mocksql[{connector}]"
            if missing.returncode == 0 or expected not in missing.stderr:
                raise RuntimeError(
                    "base install did not fail with the expected message: " + expected
                )


if __name__ == "__main__":
    main()

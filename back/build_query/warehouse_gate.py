"""Chokepoint unique pour toute requête *facturée* vers l'entrepôt réel.

Principe produit non négociable (retour session 22/07, aligné retour Niclas : la
valeur de MockSQL est le **contrôle**, pas seulement le coût) : MockSQL ne dépense
jamais un centime d'entrepôt sans l'annoncer AVANT. Chaque scan facturé passe par :

    est = estimate(sql, dialect)          # coût estimé, sans exécution facturée
    confirm_or_raise(est, ...)            # confirmation explicite (refus → exception)
    rows = run_fn()                       # la requête facturée, seulement si confirmée

- **BigQuery** : job ``dryRun=True`` → ``totalBytesProcessed`` → coût = octets × prix
  (``bq_price_per_tib``, défaut 6,25 $/TiB on-demand). Le dry-run lui-même est gratuit.
- **Snowflake** : PAS de dry-run. ``EXPLAIN USING JSON`` (métadonnées, gratuit) →
  ``bytesAssigned`` / ``partitionsAssigned`` = proxy de volume. Snowflake facture le
  **temps de warehouse** (crédits), pas les octets → JAMAIS de montant en €/$ inventé.
- **DuckDB / Postgres local** : bypass total, zéro friction (0 € facturé).

Les sites facturés (profiling, parity, futur ``inspect --live``) raccordent le gate
via ``run_gated`` (une requête) ou ``GatedExecutor`` (pattern executor du profiling).
Le fetch de schéma (métadonnées) passe une ``metadata_estimate`` → simple notice, non
bloquante, qui rend l'appel explicite (utile face au hang Snowflake live).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from storage.config import get_bq_price_per_tib
from utils.optional_deps import import_bigquery
from utils.snowflake_connector import explain_json

_BYTES_PER_TIB = 2**40
_LOCAL_DIALECTS = {"duckdb", "postgres"}
_APPROVAL_SECRET = (
    os.getenv("MOCKSQL_WAREHOUSE_APPROVAL_SECRET", "").encode()
    or secrets.token_bytes(32)
)
_APPROVAL_TTL_SECONDS = 15 * 60

# Méthodes d'estimation qui ne correspondent à AUCun scan facturé : le gate ne
# demande jamais de confirmation pour elles.
_UNBILLED_METHODS = {"local", "metadata"}


def _approval_payload(
    queries: Iterable[str], *, session: str, project: str, expires_at: int
) -> bytes:
    query_hashes = [hashlib.sha256(query.encode()).hexdigest() for query in queries]
    return json.dumps(
        {
            "expires_at": expires_at,
            "project": project,
            "query_hashes": query_hashes,
            "session": session,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def issue_approval_token(
    queries: Iterable[str], *, session: str, project: str
) -> str:
    """Sign a short-lived approval for one exact, ordered SQL batch."""
    payload = _approval_payload(
        queries,
        session=session,
        project=project,
        expires_at=int(time.time()) + _APPROVAL_TTL_SECONDS,
    )
    signature = hmac.new(_APPROVAL_SECRET, payload, hashlib.sha256).digest()
    return (
        base64.urlsafe_b64encode(payload).decode().rstrip("=")
        + "."
        + base64.urlsafe_b64encode(signature).decode().rstrip("=")
    )


def verify_approval_token(
    token: str, queries: Iterable[str], *, session: str, project: str
) -> bool:
    """Validate signature, expiry, caller scope, and exact ordered SQL."""
    try:
        encoded_payload, encoded_signature = token.split(".", 1)
        payload = base64.urlsafe_b64decode(
            encoded_payload + "=" * (-len(encoded_payload) % 4)
        )
        signature = base64.urlsafe_b64decode(
            encoded_signature + "=" * (-len(encoded_signature) % 4)
        )
        expected_signature = hmac.new(
            _APPROVAL_SECRET, payload, hashlib.sha256
        ).digest()
        if not hmac.compare_digest(signature, expected_signature):
            return False
        decoded = json.loads(payload)
        if decoded["expires_at"] < int(time.time()):
            return False
        expected_payload = _approval_payload(
            queries,
            session=session,
            project=project,
            expires_at=decoded["expires_at"],
        )
        return hmac.compare_digest(payload, expected_payload)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False


class WarehouseQueryDenied(BaseException):
    """Levée quand l'utilisateur refuse une requête entrepôt estimée.

    Hérite de ``BaseException`` (comme ``KeyboardInterrupt``) : un refus est un
    ABANDON délibéré de contrôle, pas une erreur de requête. Il ne doit JAMAIS être
    avalé par un ``except Exception`` de gestion d'erreur best-effort — typiquement les
    ``except Exception`` par-requête du ``profiler`` (row_count / colonne / jointure),
    qui logueraient un warning et poursuivraient sur un profil vide. En restant hors de
    ``Exception``, le refus traverse ces gardes et remonte directement au handler
    ``except WarehouseQueryDenied`` du call-site (``run_generate`` / ``profile`` /
    ``parity``), qui interrompt proprement — aucune requête facturée émise, pas de
    re-prompt à chaque requête suivante.
    """


@dataclass
class CostEstimate:
    """Estimation de coût d'une requête entrepôt, produite AVANT toute exécution."""

    dialect: str
    method: str  # bq_dry_run | sf_explain_json | metadata | local | unknown
    bytes_processed: int | None = None  # BQ totalBytesProcessed / SF bytesAssigned
    partitions_assigned: int | None = None  # Snowflake uniquement
    partitions_total: int | None = None  # Snowflake uniquement
    cost: float | None = None  # BigQuery uniquement (octets × prix)
    currency: str = "USD"
    context: str = ""
    warnings: list[str] = field(default_factory=list)

    @property
    def is_billed(self) -> bool:
        """True si cette estimation correspond à un scan potentiellement facturé.

        - ``local`` / ``metadata`` : jamais facturé.
        - BigQuery on-demand à **0 octet** scanné = 0 $ réellement facturé (ex.
          requête à CTEs inline, cas parity) → pas de friction inutile.
        - ``unknown`` : considéré facturé (fail-safe — mieux vaut une confirmation
          de trop qu'un scan muet). Snowflake à 0 octet reste facturé (temps de
          warehouse != octets).
        """
        if self.method in _UNBILLED_METHODS:
            return False
        if self.method == "bq_dry_run" and self.cost == 0.0:
            return False
        return True


# ── Estimation ────────────────────────────────────────────────────────────────


def estimate(
    sql: str,
    dialect: str,
    *,
    billing_project: str | None = None,
    context: str = "",
    client: object | None = None,
) -> CostEstimate:
    """Estime le coût d'une requête sans émettre de scan facturé.

    N'échoue jamais : une estimation impossible renvoie ``method="unknown"`` avec un
    warning explicatif (le call-site demandera confirmation malgré l'inconnue).

    ``client`` (BigQuery uniquement) : client réutilisable, pour éviter d'en construire
    un — et de ré-authentifier — à chaque dry-run d'un run multi-requêtes (profiling,
    parity). Absent → construit à la volée (chemin requête unique).
    """
    d = (dialect or "").lower()
    if d in _LOCAL_DIALECTS:
        return CostEstimate(dialect=d, method="local", context=context)
    if d == "bigquery":
        return _estimate_bigquery(sql, billing_project, context, client)
    if d == "snowflake":
        return _estimate_snowflake(sql, context)
    return CostEstimate(
        dialect=d,
        method="unknown",
        context=context,
        warnings=[f"Pas d'estimation de coût disponible pour le dialecte {d!r}."],
    )


def metadata_estimate(dialect: str, *, context: str = "") -> CostEstimate:
    """Estimation « métadonnées » : le fetch de schéma (INFORMATION_SCHEMA, ~gratuit).

    Non facturée → ``confirm_or_raise`` n'affiche qu'une notice, jamais un blocage.
    Rend néanmoins l'appel explicite (utile face au hang Snowflake live signalé).
    """
    return CostEstimate(
        dialect=(dialect or "").lower(), method="metadata", context=context
    )


def _estimate_bigquery(
    sql: str,
    billing_project: str | None,
    context: str,
    client: object | None = None,
) -> CostEstimate:
    try:
        bigquery = import_bigquery()
        client = client or bigquery.Client(project=billing_project)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=job_config)
        total_bytes = job.total_bytes_processed
    except Exception as exc:  # noqa: BLE001 — estimation best-effort, jamais bloquante
        return CostEstimate(
            dialect="bigquery",
            method="unknown",
            context=context,
            warnings=[f"Dry-run BigQuery impossible ({exc}) — coût inconnu."],
        )

    cost = None
    if total_bytes is not None:
        cost = (total_bytes / _BYTES_PER_TIB) * get_bq_price_per_tib()
    return CostEstimate(
        dialect="bigquery",
        method="bq_dry_run",
        bytes_processed=total_bytes,
        cost=cost,
        currency="USD",
        context=context,
        warnings=["Estimation on-demand, hors éditions/réservations."],
    )


def _estimate_snowflake(sql: str, context: str) -> CostEstimate:
    try:
        stats = explain_json(sql)
    except Exception as exc:  # noqa: BLE001 — estimation best-effort, jamais bloquante
        return CostEstimate(
            dialect="snowflake",
            method="unknown",
            context=context,
            warnings=[f"EXPLAIN Snowflake impossible ({exc}) — volume inconnu."],
        )

    def _as_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    return CostEstimate(
        dialect="snowflake",
        method="sf_explain_json",
        bytes_processed=_as_int(stats.get("bytesAssigned")),
        partitions_assigned=_as_int(stats.get("partitionsAssigned")),
        partitions_total=_as_int(stats.get("partitionsTotal")),
        cost=None,
        context=context,
        warnings=[
            "Proxy de scan (octets/partitions assignés). Coût réel = temps de "
            "warehouse (crédits), pas les octets — aucun montant affichable."
        ],
    )


# ── Formatage & confirmation ──────────────────────────────────────────────────


def _fmt_bytes(n: int | None) -> str:
    if n is None:
        return "volume inconnu"
    units = ["o", "Ko", "Mo", "Go", "To", "Po"]
    val = float(n)
    for u in units:
        if val < 1000 or u == units[-1]:
            return f"{val:.1f} {u}" if u != "o" else f"{int(val)} {u}"
        val /= 1000
    return f"{val:.1f} Po"


def format_estimate(est: CostEstimate) -> str:
    """Ligne humaine récapitulant une estimation (pour l'affichage CLI)."""
    prefix = f"{est.context} · " if est.context else ""
    if est.method == "bq_dry_run":
        money = f" · ~{est.cost:.4f} {est.currency}" if est.cost is not None else ""
        return f"{prefix}BigQuery ~{_fmt_bytes(est.bytes_processed)}{money}"
    if est.method == "sf_explain_json":
        parts = [f"Snowflake ~{_fmt_bytes(est.bytes_processed)} assignés"]
        if est.partitions_assigned is not None and est.partitions_total is not None:
            parts.append(f"{est.partitions_assigned}/{est.partitions_total} partitions")
        return prefix + " · ".join(parts) + " (coût = temps de warehouse)"
    if est.method == "metadata":
        return f"{prefix}métadonnées (schéma) — non facturé"
    if est.method == "local":
        return f"{prefix}exécution locale — 0 $ facturé"
    return (
        f"{prefix}coût inconnu — {'; '.join(est.warnings) or 'estimation indisponible'}"
    )


def auto_approve_from_env() -> bool:
    """CI / batch : ``MOCKSQL_AUTO_APPROVE_DWH=1`` approuve sans demander."""
    return os.getenv("MOCKSQL_AUTO_APPROVE_DWH", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _default_confirm(question: str) -> bool:
    import typer

    return typer.confirm(question)


def _default_echo(message: str) -> None:
    import typer

    typer.echo(message)


def confirm_or_raise(
    estimates: CostEstimate | Iterable[CostEstimate],
    *,
    auto_approve: bool = False,
    prompt_fn: Callable[[str], bool] | None = None,
    echo_fn: Callable[[str], None] | None = None,
) -> None:
    """Affiche le récap et exige une confirmation pour les scans facturés.

    - Estimations locales/métadonnées : jamais de prompt (les métadonnées émettent
      juste une notice via *echo_fn*).
    - ``auto_approve`` (flag ``--yes``) ou ``MOCKSQL_AUTO_APPROVE_DWH`` : approuve
      sans demander (les estimations restent affichées, pour la transparence).
    - Refus utilisateur → ``WarehouseQueryDenied`` (aucune requête facturée émise).
    """
    if isinstance(estimates, CostEstimate):
        estimates = [estimates]
    estimates = list(estimates)

    prompt_fn = prompt_fn or _default_confirm
    echo_fn = echo_fn or _default_echo

    for est in estimates:
        if est.method == "metadata":
            echo_fn(f"[warehouse] {format_estimate(est)}")

    billed = [e for e in estimates if e.is_billed]
    if not billed:
        return

    for est in billed:
        echo_fn(f"[warehouse] {format_estimate(est)}")
        for w in est.warnings:
            echo_fn(f"[warehouse]   ⚠ {w}")

    if auto_approve or auto_approve_from_env():
        return

    label = billed[0].context or "cette requête entrepôt"
    n = len(billed)
    question = (
        f"Exécuter {n} requête(s) facturée(s) sur l'entrepôt ({label}) ?"
        if n > 1
        else f"Exécuter {label} sur l'entrepôt ?"
    )
    if not prompt_fn(question):
        raise WarehouseQueryDenied(
            f"Requête entrepôt refusée par l'utilisateur ({label})."
        )


# ── Raccords call-site ────────────────────────────────────────────────────────


def run_gated(
    sql: str,
    dialect: str,
    run_fn: Callable[[], object],
    *,
    billing_project: str | None = None,
    context: str = "",
    auto_approve: bool = False,
    prompt_fn: Callable[[str], bool] | None = None,
    echo_fn: Callable[[str], None] | None = None,
):
    """Estime → confirme → exécute *run_fn* (la requête facturée), dans cet ordre.

    Un refus lève ``WarehouseQueryDenied`` AVANT d'appeler *run_fn* : la requête
    facturée n'est jamais émise. Convient aux sites à requête unique (parity,
    ``inspect --live``).
    """
    est = estimate(sql, dialect, billing_project=billing_project, context=context)
    confirm_or_raise(
        est, auto_approve=auto_approve, prompt_fn=prompt_fn, echo_fn=echo_fn
    )
    return run_fn()


class GatedExecutor:
    """Enveloppe un ``executor(sql) -> rows`` pour le pattern profiling.

    Le profiling génère ses requêtes à la volée (``profile_schema`` appelle
    l'executor N fois) : on ne connaît pas la liste complète d'avance. Ce wrapper
    estime et autorise donc chaque requête séparément : l'approbation d'une première
    requête ne peut pas couvrir silencieusement une requête suivante plus coûteuse.
    Un refus lève ``WarehouseQueryDenied`` avant
    d'appeler l'executor interne — aucune requête facturée émise. Sur BigQuery, le
    client (donc l'authentification) est réutilisé pour tous les dry-runs du run.
    """

    def __init__(
        self,
        inner: Callable[[str], object],
        dialect: str,
        *,
        billing_project: str | None = None,
        context: str = "profiling",
        auto_approve: bool = False,
        prompt_fn: Callable[[str], bool] | None = None,
        echo_fn: Callable[[str], None] | None = None,
    ):
        self._inner = inner
        self._dialect = dialect
        self._billing_project = billing_project
        self._context = context
        self._prompt_fn = prompt_fn
        self._echo_fn = echo_fn or _default_echo
        # --yes / env supprime uniquement le prompt. L'estimation et son affichage
        # restent obligatoires pour conserver la transparence du coût.
        self._preapproved = auto_approve or auto_approve_from_env()
        # Client BigQuery réutilisé pour tous les dry-runs du run (construit à la 1ʳᵉ
        # estimation). Un run de profiling émet des dizaines de requêtes : en reconstruire
        # un — et se ré-authentifier — à chaque estimation était un coût réseau inutile.
        self._bq_client: object | None = None

    def _bigquery_client(self) -> object | None:
        """Client BigQuery mémoïsé (dialecte bigquery seulement). Un échec de construction
        renvoie None → ``estimate`` retombe sur sa propre construction best-effort."""
        if self._dialect != "bigquery":
            return None
        if self._bq_client is None:
            try:
                self._bq_client = import_bigquery().Client(
                    project=self._billing_project
                )
            except Exception:  # noqa: BLE001 — estimate() gère l'échec (method="unknown")
                self._bq_client = None
        return self._bq_client

    def __call__(self, sql: str):
        est = estimate(
            sql,
            self._dialect,
            billing_project=self._billing_project,
            context=self._context,
            client=self._bigquery_client(),
        )
        if not est.is_billed:
            return self._inner(sql)
        # Peut lever WarehouseQueryDenied → l'executor interne n'est pas appelé.
        confirm_or_raise(
            est,
            auto_approve=self._preapproved,
            prompt_fn=self._prompt_fn,
            echo_fn=self._echo_fn,
        )
        return self._inner(sql)

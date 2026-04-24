from urllib.parse import urlparse, urlunparse

from sqlalchemy import create_engine, text


def ensure_pg8000_scheme(db_url: str) -> str:
    """
    Normalise une URL Postgres pour SQLAlchemy+pg8000.
    """
    url = db_url
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and not url.startswith("postgresql+pg8000://"):
        url = url.replace("postgresql://", "postgresql+pg8000://", 1)
    return url


def test_connection(db_url: str) -> None:
    """
    Teste une URL Postgres en exécutant SELECT 1.
    """
    url = ensure_pg8000_scheme(db_url)
    engine = create_engine(url, future=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def mask_credentials_in_url(db_url: str) -> str:
    """
    Masque le mot de passe dans une URL de connexion.
    """
    parsed = urlparse(db_url)
    if not parsed.username or not parsed.password:
        return db_url
    masked = "*" * 5
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    new_netloc = f"{parsed.username}:{masked}@{host}{port}"
    return urlunparse(parsed._replace(netloc=new_netloc))

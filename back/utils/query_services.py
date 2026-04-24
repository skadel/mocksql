from sqlalchemy import create_engine, text


# --- PostgreSQL services ---
def ensure_pg8000_scheme(db_url: str) -> str:
    url = db_url
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and not url.startswith("postgresql+pg8000://"):
        url = url.replace("postgresql://", "postgresql+pg8000://", 1)
    return url


def test_connection(db_url: str) -> None:
    engine = create_engine(db_url, future=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

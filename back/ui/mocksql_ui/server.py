import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from langserve import add_routes
from starlette.middleware.cors import CORSMiddleware

from app.api.endpoints import query, messages, models, projects, users
from build_query.query_chain import build_query_graph
from models.database import db_pool

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

app = FastAPI()


def get_static_dir() -> Path:
    if getattr(sys, "frozen", False):  # PyInstaller one-file executable
        return Path(sys._MEIPASS) / "static"
    return Path(__file__).parent / "static"


app.mount("/static", StaticFiles(directory=str(get_static_dir())), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("FRONT_URL")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

api_router = APIRouter(prefix="/api")

api_router.include_router(query.router)
api_router.include_router(messages.router)
api_router.include_router(models.router)
api_router.include_router(projects.router)
api_router.include_router(users.router)

add_routes(api_router, build_query_graph(), path="/query/build")

app.include_router(api_router)


@app.on_event("startup")
async def on_startup():
    logging.info("Démarrage de l'application…")
    await db_pool.init_pool()
    from init.init_db import run_migrations

    await run_migrations()


@app.on_event("shutdown")
async def on_shutdown():
    logging.info("Fermeture de l'application…")
    await db_pool.close()


@app.get("/{full_path:path}")
async def serve_spa(full_path: str, request: Request):
    if request.url.path.startswith("/api"):
        raise HTTPException(status_code=404, detail="Not Found")
    file_path = get_static_dir() / full_path
    if file_path.exists():
        return FileResponse(str(file_path))
    return FileResponse(str(get_static_dir() / "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)), reload=False)

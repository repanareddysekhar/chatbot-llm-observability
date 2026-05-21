from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import engine, Base
from .routers import health, ingest, logs, metrics, stream


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables if they don't exist (idempotent — migrations handle schema)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="LLM Observability — Ingestion API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/v1")
app.include_router(ingest.router, prefix="/v1")
app.include_router(logs.router, prefix="/v1")
app.include_router(metrics.router, prefix="/v1")
app.include_router(stream.router, prefix="/v1")

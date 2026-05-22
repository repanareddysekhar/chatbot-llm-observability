import json
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

# Load .env into os.environ before any SDK or discovery code runs.
# web/.env (local dev) → repo-root .env as fallback.
load_dotenv(Path(__file__).parent.parent / ".env")           # web/.env (local)
load_dotenv(Path(__file__).parent.parent.parent / ".env", override=False)  # root .env

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import settings
from .database import engine, Base
from llm_obs import available_providers
from .routers import chat, conversations, dashboard


try:
    from llm_obs import ObservabilityClient
    _obs = ObservabilityClient(
        endpoint=settings.ingest_url,
        api_key=settings.ingest_api_key,
        environment=settings.environment,
        redact_pii=True,
    )
    _obs.auto_instrument()  # patches openai / anthropic / gemini / bedrock at class level
except Exception:
    pass  # observability is never allowed to break the app


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="LLM Observability — Web", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

templates = Jinja2Templates(directory="app/templates")

# API routers
app.include_router(chat.router)
app.include_router(conversations.router)
app.include_router(dashboard.router)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/chat")


@app.get("/chat", response_class=HTMLResponse)
@app.get("/chat/{conv_id}", response_class=HTMLResponse)
async def chat_page(request: Request, conv_id: str | None = None):
    providers = available_providers()
    if not providers:
        providers = {"ollama": ["gemma3:4b", "llama3.2", "mistral"]}
    return templates.TemplateResponse(
        request,
        "chat.html",
        {
            "active": "chat",
            "conv_id": conv_id or "",
            "providers": providers,
            "providers_json": json.dumps(providers),
        },
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse(
        request, "dashboard.html", {"active": "dashboard"}
    )


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    return templates.TemplateResponse(
        request, "logs.html", {"active": "logs"}
    )


# SSE chat endpoint via GET (for EventSource compatibility)
from .routers.chat import ChatRequest
from sse_starlette.sse import EventSourceResponse
from .routers.chat import router as _chat_router


@app.get("/api/chat-stream")
async def chat_stream(
    request: Request,
    message: str,
    provider: str = "openai",
    model: str = "gpt-4o-mini",
    conversation_id: str = "",
):
    """GET-based SSE endpoint for EventSource (browser native)."""
    from .routers.chat import chat as chat_handler
    from .database import AsyncSessionLocal
    from pydantic import BaseModel

    class FakeBody(BaseModel):
        conversation_id: str | None
        message: str
        provider: str
        model: str

    body = FakeBody(
        conversation_id=conversation_id or None,
        message=message,
        provider=provider,
        model=model,
    )
    async with AsyncSessionLocal() as db:
        return await chat_handler(body, db)

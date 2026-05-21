import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import settings
from .database import engine, Base
from .llm.factory import get_provider_client
from .routers import chat, conversations, dashboard


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tables are created by ingestion service; web only reads
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
    providers = get_provider_client()
    if not providers:
        # If nothing is configured, at least show Ollama with common models
        # so the UI is usable without any cloud API keys
        providers = {"ollama": ["gemma3:4b", "llama3.2", "mistral"]}
    return templates.TemplateResponse(
        "chat.html",
        {
            "request": request,
            "active": "chat",
            "conv_id": conv_id or "",
            "providers": providers,
            "providers_json": json.dumps(providers),
        },
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "active": "dashboard"}
    )


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    return templates.TemplateResponse(
        "logs.html", {"request": request, "active": "logs"}
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

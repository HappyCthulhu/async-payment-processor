"""Local, in-memory webhook receiver and browser demo; not a production service."""

import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

app = FastAPI(title="Demo webhook sink")
callbacks: list[dict[str, Any]] = []
attempts: defaultdict[str, int] = defaultdict(int)
sequence = [int(value.strip()) for value in os.getenv("WEBHOOK_RESPONSE_SEQUENCE", "204").split(",")]
if not sequence or any(code not in range(200, 600) for code in sequence):
    raise ValueError("WEBHOOK_RESPONSE_SEQUENCE must contain HTTP statuses from 200 to 599")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return Path(__file__).with_name("index.html").read_text(encoding="utf-8")


@app.post("/callbacks/payments")
async def callback(request: Request) -> Response:
    payload = await request.json()
    key = str(payload.get("payment_id", "unknown"))
    attempt = attempts[key]
    attempts[key] += 1
    code = sequence[min(attempt, len(sequence) - 1)]
    callbacks.append({"received_at": datetime.now(UTC).isoformat(), "response_status": code, "payload": payload})
    return Response(status_code=code)


@app.get("/callbacks")
async def list_callbacks() -> list[dict[str, Any]]:
    return callbacks


@app.post("/demo/payments")
async def create_payment(request: Request, idempotency_key: Annotated[str, Header()]) -> Response:
    return await proxy("POST", "/api/v1/payments", await request.json(), idempotency_key)


@app.get("/demo/payments/{payment_id}")
async def get_payment(payment_id: str) -> Response:
    return await proxy("GET", f"/api/v1/payments/{payment_id}")


async def proxy(method: str, path: str, body: object = None, key: str = "") -> Response:
    headers = {"X-API-Key": os.getenv("APP_API_KEY", "local-development-key")}
    if key:
        headers["Idempotency-Key"] = key
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            result = await client.request(
                method, os.getenv("DEMO_API_URL", "http://api:8000") + path, json=body, headers=headers
            )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Payment API is unavailable") from exc
    return Response(content=result.content, status_code=result.status_code, media_type="application/json")

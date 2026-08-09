from __future__ import annotations

import asyncio
from collections.abc import Callable

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from funding_arbitrage.monitoring.metrics import websocket_connections

router = APIRouter()


async def _stream(websocket: WebSocket, payload_factory: Callable[[], object]) -> None:
    await websocket.accept()
    websocket_connections.inc()
    try:
        while True:
            await websocket.send_json(payload_factory())
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=5)
            except TimeoutError:
                continue
    except WebSocketDisconnect:
        return
    finally:
        websocket_connections.dec()


@router.websocket("/ws/opportunities")
async def opportunities_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket,
        lambda: [
            item.model_dump(mode="json") for item in websocket.app.state.runtime.opportunities
        ],
    )


@router.websocket("/ws/portfolio")
async def portfolio_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket, lambda: websocket.app.state.runtime.portfolio.snapshot().model_dump(mode="json")
    )


@router.websocket("/ws/market")
async def market_socket(websocket: WebSocket) -> None:
    await _stream(
        websocket,
        lambda: {
            "captured_at": websocket.app.state.runtime.latest_snapshot.captured_at.isoformat()
            if websocket.app.state.runtime.latest_snapshot
            else None
        },
    )

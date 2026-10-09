"""Read-only Telegram credential and report-destination probe.

The probe calls only getMe/getChat and never sends a message. Its output is
deliberately redacted so the bot token cannot appear in success or error data.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx

from funding_arbitrage.config import Settings, get_settings


async def _telegram_call(
    client: httpx.AsyncClient,
    token: str,
    method: str,
    *,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        response = await client.get(f"/bot{token}/{method}", params=params)
    except httpx.HTTPError as error:
        return {"ok": False, "error_type": type(error).__name__}
    try:
        payload = response.json()
    except ValueError:
        return {
            "ok": False,
            "http_status": response.status_code,
            "error_type": "InvalidJson",
        }
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return {
            "ok": False,
            "http_status": response.status_code,
            "error_code": payload.get("error_code") if isinstance(payload, dict) else None,
        }
    result = payload.get("result")
    return {"ok": True, "result": result if isinstance(result, dict) else {}}


async def probe_telegram(
    settings: Settings,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    token = settings.telegram_bot_token.get_secret_value()
    configured = bool(token and settings.telegram_chat_id)
    output: dict[str, Any] = {
        "observed_at": datetime.now(UTC).isoformat(),
        "enabled": settings.telegram_enabled,
        "token_configured": bool(token),
        "chat_id_configured": bool(settings.telegram_chat_id),
        "report_timezone": settings.telegram_timezone,
        "report_hour": settings.telegram_report_hour,
        "report_minute": settings.telegram_report_minute,
    }
    if not settings.telegram_enabled or not configured:
        output["ok"] = False
        return output

    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(
        base_url=settings.telegram_api_base_url,
        timeout=10,
    )
    try:
        bot = await _telegram_call(client, token, "getMe")
        chat = await _telegram_call(
            client,
            token,
            "getChat",
            params={"chat_id": settings.telegram_chat_id},
        )
    finally:
        if owns_client:
            await client.aclose()

    bot_result = bot.get("result") if bot.get("ok") is True else {}
    chat_result = chat.get("result") if chat.get("ok") is True else {}
    bot_result = bot_result if isinstance(bot_result, dict) else {}
    chat_result = chat_result if isinstance(chat_result, dict) else {}
    output.update(
        {
            "get_me_ok": bot.get("ok") is True,
            "get_chat_ok": chat.get("ok") is True,
            "bot_id_present": bot_result.get("id") is not None,
            "bot_username_present": bool(bot_result.get("username")),
            "chat_id_matches": str(chat_result.get("id")) == settings.telegram_chat_id,
            "chat_type": chat_result.get("type"),
            "get_me_error_type": bot.get("error_type"),
            "get_chat_error_type": chat.get("error_type"),
        }
    )
    output["ok"] = all(
        output[key] is True
        for key in (
            "enabled",
            "token_configured",
            "chat_id_configured",
            "get_me_ok",
            "get_chat_ok",
            "bot_id_present",
            "bot_username_present",
            "chat_id_matches",
        )
    )
    return output


async def main() -> int:
    output = await probe_telegram(get_settings())
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

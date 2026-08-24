import json

import httpx
import pytest
from scripts.telegram_config_probe import probe_telegram

from funding_arbitrage.config import Settings


def _settings() -> Settings:
    return Settings(
        TELEGRAM_ENABLED=True,
        TELEGRAM_BOT_TOKEN="secret-test-token",
        TELEGRAM_CHAT_ID="316196998",
        TELEGRAM_TIMEZONE="Europe/Kyiv",
        TELEGRAM_REPORT_HOUR=0,
        TELEGRAM_REPORT_MINUTE=0,
    )


@pytest.mark.asyncio
async def test_probe_validates_bot_and_chat_without_sending_message() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert "/botsecret-test-token/" in request.url.path
        assert "**********" not in request.url.path
        methods.append(request.url.path.rsplit("/", 1)[-1])
        if request.url.path.endswith("/getMe"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"id": 42, "username": "paper_bot"}},
            )
        return httpx.Response(
            200,
            json={"ok": True, "result": {"id": 316196998, "type": "private"}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.telegram.org",
    ) as client:
        result = await probe_telegram(_settings(), http_client=client)

    assert methods == ["getMe", "getChat"]
    assert result["ok"] is True
    assert result["chat_id_matches"] is True
    assert result["report_timezone"] == "Europe/Kyiv"


@pytest.mark.asyncio
async def test_probe_redacts_token_when_transport_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("secret-test-token must not escape", request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.telegram.org",
    ) as client:
        result = await probe_telegram(_settings(), http_client=client)

    serialized = json.dumps(result)
    assert result["ok"] is False
    assert result["get_me_error_type"] == "ConnectError"
    assert "secret-test-token" not in serialized

"""Minimal Telegram Bot API adapter with secret-safe error handling.

The bot token is part of the request URL, so no exception that could carry the
URL (httpx errors do) is chained, logged, or re-raised from here.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from funding_arbitrage.monitoring.metrics import telegram_messages_total

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 4096


class TelegramNotificationError(RuntimeError):
    """Telegram rejected or failed a notification request."""


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        api_base_url: str = "https://api.telegram.org",
        timeout_seconds: float = 10.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    async def send_message(self, text: str) -> None:
        if not self.configured:
            raise TelegramNotificationError("Telegram notifier is not configured")
        if len(text) > MAX_MESSAGE_LENGTH:
            text = text[: MAX_MESSAGE_LENGTH - 1] + "…"
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout_seconds)
        url = f"{self.api_base_url}/bot{self.bot_token}/sendMessage"
        try:
            response = await self._http.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True},
            )
        except httpx.HTTPError as exc:
            raise TelegramNotificationError(
                f"Telegram request failed: {type(exc).__name__}"
            ) from None
        try:
            payload: Any = response.json()
        except ValueError:
            payload = None
        if (
            response.status_code != 200
            or not isinstance(payload, dict)
            or payload.get("ok") is not True
        ):
            description = payload.get("description") if isinstance(payload, dict) else None
            raise TelegramNotificationError(
                f"Telegram returned HTTP {response.status_code}: {str(description or '')[:120]}"
            )

    async def send_notice(self, kind: str, text: str) -> bool:
        """Best-effort notice (start/stop): failures are logged, never raised."""

        try:
            await self.send_message(text)
        except TelegramNotificationError as exc:
            telegram_messages_total.labels(kind, "failed").inc()
            logger.warning("telegram_notice_failed", extra={"kind": kind, "error": str(exc)})
            return False
        telegram_messages_total.labels(kind, "sent").inc()
        return True

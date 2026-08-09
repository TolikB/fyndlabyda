"""Minimal Telegram Bot API adapter with secret-safe error handling."""

from __future__ import annotations

from typing import Any

import httpx


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
        if len(text) > 4096:
            text = text[:4090] + "…"
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout_seconds)
        url = f"{self.api_base_url}/bot{self.bot_token}/sendMessage"
        try:
            response = await self._http.post(
                url,
                json={"chat_id": self.chat_id, "text": text},
            )
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelegramNotificationError("Telegram request failed") from exc
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            raise TelegramNotificationError("Telegram returned an unsuccessful response")

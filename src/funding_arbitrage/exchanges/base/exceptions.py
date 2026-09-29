"""Exchange failures exposed to the application layer."""


class ExchangeError(Exception):
    """Base exchange adapter error."""


class NetworkError(ExchangeError):
    """The exchange could not be reached."""


class RateLimitError(ExchangeError):
    """The exchange rejected a request because of a rate limit."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class InvalidResponseError(ExchangeError):
    """The exchange response was malformed or rejected."""


class StaleDataError(ExchangeError):
    """A market-data item was too old to use."""


class SymbolMappingError(ExchangeError):
    """An exchange symbol could not be normalized."""

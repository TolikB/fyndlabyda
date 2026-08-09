"""Exchange failures exposed to the application layer."""


class ExchangeError(Exception):
    """Base exchange adapter error."""


class NetworkError(ExchangeError):
    """The exchange could not be reached."""


class RateLimitError(ExchangeError):
    """The exchange rejected a request because of a rate limit."""


class InvalidResponseError(ExchangeError):
    """The exchange response was malformed or rejected."""


class StaleDataError(ExchangeError):
    """A market-data item was too old to use."""


class SymbolMappingError(ExchangeError):
    """An exchange symbol could not be normalized."""

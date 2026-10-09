COPY (
    SELECT
        exchange,
        symbol,
        instrument_type,
        interval_minutes,
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        is_closed
    FROM market_candles
    WHERE close_time >= TIMESTAMPTZ '2026-07-12 00:00:00+00'
      AND close_time < TIMESTAMPTZ '2026-08-11 00:00:00+00'
      AND is_closed IS TRUE
    ORDER BY close_time, exchange, symbol, instrument_type
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);

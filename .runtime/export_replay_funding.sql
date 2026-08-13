COPY (
    SELECT
        funding.exchange,
        funding.symbol,
        funding.funding_rate,
        funding.funding_timestamp,
        funding.mark_price
    FROM funding_history AS funding
    WHERE funding.funding_timestamp >= TIMESTAMPTZ '2026-06-12 00:00:00+00'
      AND funding.funding_timestamp < TIMESTAMPTZ '2026-08-11 00:00:00+00'
      AND EXISTS (
          SELECT 1
          FROM market_candles AS candle
          WHERE candle.exchange = funding.exchange
            AND candle.symbol = funding.symbol
            AND candle.instrument_type = 'PERPETUAL'
            AND candle.close_time >= TIMESTAMPTZ '2026-07-12 00:00:00+00'
            AND candle.close_time < TIMESTAMPTZ '2026-08-11 00:00:00+00'
            AND candle.is_closed IS TRUE
      )
    ORDER BY funding.funding_timestamp, funding.exchange, funding.symbol
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);

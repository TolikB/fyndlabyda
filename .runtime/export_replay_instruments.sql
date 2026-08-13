COPY (
    SELECT
        instrument.exchange,
        instrument.exchange_symbol,
        instrument.base_asset,
        instrument.quote_asset,
        instrument.instrument_type,
        instrument.settlement_asset,
        instrument.contract_size,
        instrument.tick_size,
        instrument.step_size,
        instrument.min_order_size,
        instrument.funding_interval,
        instrument.expiry,
        instrument.is_active
    FROM instruments AS instrument
    WHERE EXISTS (
        SELECT 1
        FROM market_candles AS candle
        WHERE candle.exchange = instrument.exchange
          AND candle.symbol = instrument.exchange_symbol
          AND candle.instrument_type = instrument.instrument_type
          AND candle.close_time >= TIMESTAMPTZ '2026-07-12 00:00:00+00'
          AND candle.close_time < TIMESTAMPTZ '2026-08-11 00:00:00+00'
          AND candle.is_closed IS TRUE
    )
    ORDER BY instrument.exchange, instrument.exchange_symbol, instrument.instrument_type
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);

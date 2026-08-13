import asyncio
from datetime import date

from funding_arbitrage.config import get_settings
from funding_arbitrage.database.session import create_database
from funding_arbitrage.services.daily_report import DailyReportService


async def main() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    service = DailyReportService(settings, session_factory)
    try:
        async with session_factory() as session:
            print(await service._build_message(session, date(2026, 8, 11)))
    finally:
        await service.close()
        await engine.dispose()


asyncio.run(main())

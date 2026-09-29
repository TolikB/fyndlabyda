from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from funding_arbitrage.database.models import Base

config = context.config
if config.config_file_name is not None and config.get_section("loggers"):
    fileConfig(config.config_file_name)
target_metadata = Base.metadata

# The application's DATABASE_URL wins over alembic.ini so migrations run against the
# same database as the service (inside Docker the host is "postgres", not localhost).
_database_url = os.environ.get("DATABASE_URL")
if not _database_url:
    try:
        from funding_arbitrage.config import get_settings

        _database_url = get_settings().database_url
    except Exception:  # noqa: BLE001 - fall back to alembic.ini
        _database_url = None
if _database_url:
    config.set_main_option("sqlalchemy.url", _database_url.replace("%", "%%"))


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

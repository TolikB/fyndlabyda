from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from funding_arbitrage.database.models import Base
from funding_arbitrage.internal_tls import create_client_ssl_context

config = context.config
database_url = os.getenv("DATABASE_URL")
if database_url:
    # ConfigParser treats percent signs as interpolation tokens. Escape them so
    # URL-encoded passwords remain valid when supplied through the environment.
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
if config.config_file_name is not None and config.get_section("loggers"):
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


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
    connect_args: dict[str, object] = {}
    if os.getenv("INTERNAL_SERVICE_TLS_REQUIRED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        connect_args["ssl"] = create_client_ssl_context(
            ca_file=os.environ["INTERNAL_TLS_CA_FILE"],
            certificate_file=os.environ["INTERNAL_TLS_CLIENT_CERT_FILE"],
            key_file=os.environ["INTERNAL_TLS_CLIENT_KEY_FILE"],
        )
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=connect_args,
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

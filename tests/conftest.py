from collections.abc import AsyncIterator
from uuid import uuid4

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from payment_processor.core.config import settings
from payment_processor.infrastructure.db.base import BaseModel


@pytest_asyncio.fixture
async def db_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    schema_name = f"test_{uuid4().hex}"
    engine = create_async_engine(
        settings.database_url,
        connect_args={"server_settings": {"search_path": schema_name}},
    )

    async with engine.begin() as connection:
        await connection.execute(CreateSchema(schema_name))
        await connection.run_sync(BaseModel.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True))
        await engine.dispose()

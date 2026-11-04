from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment_processor.core.config import settings

engine = create_async_engine(settings.database_url, echo=settings.db_echo)

session_factory = async_sessionmaker(engine, expire_on_commit=False)

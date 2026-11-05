from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Annotated

from fastapi import Header, HTTPException, status

from payment_processor.core.config import settings
from payment_processor.infrastructure.db.session import session_factory

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def verify_api_key(api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> None:
    if api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key!")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session

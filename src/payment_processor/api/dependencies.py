from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: TC002

from payment_processor.application.use_cases.create_payment import CreatePaymentUseCase
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository
from payment_processor.infrastructure.db.session import session_factory


def verify_api_key(api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> None:
    if api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key!")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


def get_payment_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SqlAlchemyPaymentRepository:
    return SqlAlchemyPaymentRepository(session)


def get_create_payment_use_case(
    repository: Annotated[SqlAlchemyPaymentRepository, Depends(get_payment_repository)],
) -> CreatePaymentUseCase:
    return CreatePaymentUseCase(repository)

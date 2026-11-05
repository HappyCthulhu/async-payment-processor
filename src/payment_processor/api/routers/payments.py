from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from payment_processor.api.dependencies import get_create_payment_use_case, verify_api_key
from payment_processor.api.schemas import CreatePaymentRequest, CreatePaymentResponse
from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.errors import IdempotencyConflictError
from payment_processor.application.results import PaymentResult
from payment_processor.application.use_cases.create_payment import CreatePaymentUseCase

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(verify_api_key)])


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=CreatePaymentResponse)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    use_case: Annotated[CreatePaymentUseCase, Depends(get_create_payment_use_case)],
) -> PaymentResult:
    command = CreatePaymentCommand(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
    )

    try:
        return await use_case.execute(command)
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key is already used with different payment data.",
        ) from error

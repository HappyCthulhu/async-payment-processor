from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from payment_processor.api.dependencies import get_create_payment_use_case, get_payment_by_id_use_case, verify_api_key
from payment_processor.api.schemas import CreatePaymentRequest, CreatePaymentResponse, GetPaymentByIdResponse
from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.errors import IdempotencyConflictError
from payment_processor.application.results import PaymentDetails, PaymentResult
from payment_processor.application.use_cases.create_payment import CreatePaymentUseCase
from payment_processor.application.use_cases.get_payment_by_id import GetPaymentUseCase

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


@router.get("/{payment_id}", response_model=GetPaymentByIdResponse)
async def get_payment(
    payment_id: UUID, use_case: Annotated[GetPaymentUseCase, Depends(get_payment_by_id_use_case)]
) -> PaymentDetails:
    result = await use_case.get_by_id(payment_id)

    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="There is no payment for this ID")
    return result

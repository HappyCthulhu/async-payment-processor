from fastapi import APIRouter, Depends

from payment_processor.api.dependencies import verify_api_key

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(verify_api_key)])

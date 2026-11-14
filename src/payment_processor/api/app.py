from fastapi import FastAPI

from payment_processor.api.lifespan import lifespan
from payment_processor.api.routers.payments import router as payments_router


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Processor", version="0.1.0", lifespan=lifespan)
    app.include_router(payments_router)
    return app


app = create_app()

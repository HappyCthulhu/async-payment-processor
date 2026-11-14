from typing import Literal

type FinalPaymentStatus = Literal["succeeded", "failed"]

type PaymentStatus = Literal["succeeded", "failed", "pending"]

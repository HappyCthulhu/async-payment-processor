from typing import Literal

type Currency = Literal["RUB", "USD", "EUR"]

type FinalPaymentStatus = Literal["succeeded", "failed"]

type PaymentStatus = Literal["pending", "succeeded", "failed"]

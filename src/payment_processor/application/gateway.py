from enum import StrEnum


class GatewayOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TEMPORARY_ERROR = "temporary_error"

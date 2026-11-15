"""Separate the broker delivery marker from the webhook POST count.

Historical webhook_attempts values are broker attempt numbers. They cannot be
converted into exact POST counts when gateway failures consumed earlier attempts.
Preserve them as-is and copy them to the marker to avoid replaying old attempts.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b8c92b153d42"
down_revision: str | Sequence[str] | None = "a9ed5758e9b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("last_recorded_webhook_attempt", sa.Integer(), nullable=False, server_default="0"),
    )
    op.execute("UPDATE payments SET last_recorded_webhook_attempt = webhook_attempts")


def downgrade() -> None:
    # The old application expects the delivery marker in webhook_attempts.
    op.execute("UPDATE payments SET webhook_attempts = last_recorded_webhook_attempt")
    op.drop_column("payments", "last_recorded_webhook_attempt")

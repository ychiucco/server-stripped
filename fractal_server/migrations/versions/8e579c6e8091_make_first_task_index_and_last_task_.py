"""Make first_task_index and last_task_index required

Revision ID: 8e579c6e8091
Revises: 8f79bd162e35
Create Date: 2023-09-12 08:07:57.944760

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "8e579c6e8091"
down_revision = "8f79bd162e35"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.alter_column(
            "first_task_index", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column(
            "last_task_index", existing_type=sa.INTEGER(), nullable=False
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.alter_column(
            "last_task_index", existing_type=sa.INTEGER(), nullable=True
        )
        batch_op.alter_column(
            "first_task_index", existing_type=sa.INTEGER(), nullable=True
        )

    # ### end Alembic commands ###

"""Elijah added historical subscriber count.

Revision ID: 880b0d854bfc
Revises: f4f984fc8de6
Create Date: 2024-04-30 18:11:00.583696

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '880b0d854bfc'
down_revision: Union[str, None] = 'f4f984fc8de6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('channels_historical_stats',
    sa.Column('stat_id', sa.String(), nullable=False),
    sa.Column('channel_id', sa.String(), nullable=True),
    sa.Column('date', sa.TIMESTAMP(), nullable=True),
    sa.Column('subs', sa.Integer(), nullable=True),
    sa.Column('views', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('stat_id')
    )
    op.create_table('videos_evaluated',
    sa.Column('video_id', sa.String(), nullable=False),
    sa.Column('score', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('video_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('videos_evaluated')
    op.drop_table('channels_historical_stats')
    # ### end Alembic commands ###

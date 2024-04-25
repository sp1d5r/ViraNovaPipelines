"""Elijah - added embedding tables.

Revision ID: 86d25a0e4b92
Revises: 1e475373c285
Create Date: 2024-04-25 12:09:28.102153

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '86d25a0e4b92'
down_revision: Union[str, None] = '1e475373c285'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('embeddings_segment',
    sa.Column('segment_id', sa.String(), nullable=False),
    sa.Column('video_id', sa.String(), nullable=True),
    sa.Column('embedding', sa.LargeBinary(), nullable=True),
    sa.PrimaryKeyConstraint('segment_id')
    )
    op.create_table('embeddings_transcript',
    sa.Column('video_id', sa.String(), nullable=False),
    sa.Column('embedding', sa.LargeBinary(), nullable=True),
    sa.PrimaryKeyConstraint('video_id')
    )
    op.create_table('transcripts_segment_embedded',
    sa.Column('segment_id', sa.String(), nullable=False),
    sa.Column('video_id', sa.String(), nullable=True),
    sa.Column('embedded_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('segment_id')
    )
    op.create_table('videos_embedded',
    sa.Column('video_id', sa.String(), nullable=False),
    sa.Column('embedded_at', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('video_id')
    )
    op.alter_column('channels_downloaded', 'channel_id',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               nullable=False)
    op.alter_column('channels_downloaded', 'channel_link',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('channels_downloaded', 'channel_name',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('channels_downloaded', 'channel_name',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('channels_downloaded', 'channel_link',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('channels_downloaded', 'channel_id',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               nullable=True)
    op.drop_table('videos_embedded')
    op.drop_table('transcripts_segment_embedded')
    op.drop_table('embeddings_transcript')
    op.drop_table('embeddings_segment')
    # ### end Alembic commands ###

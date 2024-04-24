"""Elijah added videos segmented table.

Revision ID: 3d155c89653a
Revises: 239551ec25eb
Create Date: 2024-04-24 23:25:09.552581

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3d155c89653a'
down_revision: Union[str, None] = '239551ec25eb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
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
    op.alter_column('videos_cleaned', 'video_id',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               nullable=False)
    op.alter_column('videos_cleaned', 'channel_id',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'title',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'description',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'channel_title',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'publish_time',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'width',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'height',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'aspect_ratio',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'view_count',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'like_count',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'favorite_count',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'comment_count',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'duration',
               existing_type=sa.BIGINT(),
               type_=sa.Integer(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'projection',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
    op.drop_column('videos_cleaned', 'download_link')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('videos_cleaned', sa.Column('download_link', sa.TEXT(), autoincrement=False, nullable=True))
    op.alter_column('videos_cleaned', 'projection',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'duration',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'comment_count',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'favorite_count',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'like_count',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'view_count',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'aspect_ratio',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'height',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'width',
               existing_type=sa.Integer(),
               type_=sa.BIGINT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'publish_time',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'channel_title',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'description',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'title',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'channel_id',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.alter_column('videos_cleaned', 'video_id',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               nullable=True)
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
    # ### end Alembic commands ###

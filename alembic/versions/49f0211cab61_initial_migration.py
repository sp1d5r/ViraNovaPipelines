"""Initial migration

Revision ID: 49f0211cab61
Revises: 
Create Date: 2024-04-19 15:48:56.311330

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '49f0211cab61'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('channels_downloaded',
    sa.Column('channel_id', sa.String(), nullable=False),
    sa.Column('channel_link', sa.String(), nullable=True),
    sa.Column('channel_name', sa.String(), nullable=True),
    sa.Column('downloaded', sa.Boolean(), nullable=True),
    sa.Column('date_added', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('channel_id')
    )
    op.create_table('channels_raw',
    sa.Column('channel_link', sa.String(), nullable=True),
    sa.Column('channel_id', sa.String(), nullable=False),
    sa.Column('channel_name', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('channel_id')
    )
    op.create_table('videos_cleaned',
    sa.Column('videoId', sa.String(), nullable=False),
    sa.Column('channel_id', sa.String(), nullable=True),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('channelTitle', sa.String(), nullable=True),
    sa.Column('publishTime', sa.String(), nullable=True),
    sa.Column('width', sa.Integer(), nullable=True),
    sa.Column('height', sa.Integer(), nullable=True),
    sa.Column('aspect_ratio', sa.Integer(), nullable=True),
    sa.Column('viewCount', sa.Integer(), nullable=True),
    sa.Column('likeCount', sa.Integer(), nullable=True),
    sa.Column('favoriteCount', sa.Integer(), nullable=True),
    sa.Column('commentCount', sa.Integer(), nullable=True),
    sa.Column('duration', sa.Integer(), nullable=True),
    sa.Column('projection', sa.String(), nullable=True),
    sa.Column('date_added', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('videoId')
    )
    op.create_table('videos_downloaded',
    sa.Column('videoId', sa.String(), nullable=False),
    sa.Column('video_downloaded', sa.Boolean(), nullable=True),
    sa.Column('audio_downloaded', sa.Boolean(), nullable=True),
    sa.Column('video_downloaded_path', sa.String(), nullable=True),
    sa.Column('audio_downloaded_path', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('videoId')
    )
    op.create_table('videos_raw',
    sa.Column('videoId', sa.String(), nullable=False),
    sa.Column('channel_id', sa.String(), nullable=True),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('channelTitle', sa.String(), nullable=True),
    sa.Column('publishTime', sa.String(), nullable=True),
    sa.Column('width', sa.Integer(), nullable=True),
    sa.Column('height', sa.Integer(), nullable=True),
    sa.Column('aspect_ratio', sa.Integer(), nullable=True),
    sa.Column('viewCount', sa.String(), nullable=True),
    sa.Column('likeCount', sa.String(), nullable=True),
    sa.Column('favoriteCount', sa.String(), nullable=True),
    sa.Column('commentCount', sa.String(), nullable=True),
    sa.Column('duration', sa.String(), nullable=True),
    sa.Column('projection', sa.String(), nullable=True),
    sa.Column('date_added', sa.TIMESTAMP(), nullable=True),
    sa.PrimaryKeyConstraint('videoId')
    )
    op.create_table('videos_type',
    sa.Column('videoId', sa.String(), nullable=False),
    sa.Column('aspect_ratio', sa.Float(), nullable=True),
    sa.Column('num_frames', sa.Integer(), nullable=True),
    sa.Column('is_short', sa.Boolean(), nullable=True),
    sa.PrimaryKeyConstraint('videoId')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('videos_type')
    op.drop_table('videos_raw')
    op.drop_table('videos_downloaded')
    op.drop_table('videos_cleaned')
    op.drop_table('channels_raw')
    op.drop_table('channels_downloaded')
    # ### end Alembic commands ###

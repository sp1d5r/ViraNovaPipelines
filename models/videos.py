from database.production_db.database import Base
from sqlalchemy import Column, Integer, BIGINT, String, TIMESTAMP, Boolean, Float
from table_names import (
    videos_raw,
    videos_cleaned,
    videos_downloaded,
    videos_type,
    videos_transcribed,
    videos_segmented,
    videos_embedded,
    videos_vectors_stored,
    videos_evaluated
)


class VideosRaw(Base):
    __tablename__ = videos_raw

    videoId = Column(String, primary_key=True)
    channel_id = Column(String)
    title = Column(String)
    description = Column(String)
    channelTitle = Column(String)
    publishTime = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    aspect_ratio = Column(Integer)
    viewCount = Column(String)
    likeCount = Column(String)
    favoriteCount = Column(String)
    commentCount = Column(String)
    duration = Column(String)
    projection = Column(String)
    date_added = Column(TIMESTAMP)


class VideosCleaned(Base):
    __tablename__ = videos_cleaned

    video_id = Column(String, primary_key=True)
    channel_id = Column(String)
    title = Column(String)
    description = Column(String)
    channel_title = Column(String)
    publish_time = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    aspect_ratio = Column(Integer)
    view_count = Column(BIGINT)
    like_count = Column(BIGINT)
    favorite_count = Column(BIGINT)
    comment_count = Column(BIGINT)
    duration = Column(BIGINT)
    projection = Column(String)
    date_added = Column(TIMESTAMP)


class VideosDownloaded(Base):
    __tablename__ = videos_downloaded

    video_id = Column(String, primary_key=True)
    video_downloaded = Column(Boolean)
    audio_downloaded = Column(Boolean)
    video_downloaded_path = Column(String)
    audio_downloaded_path = Column(String)


class VideosType(Base):
    __tablename__ = videos_type

    video_id = Column(String, primary_key=True)
    aspect_ratio = Column(Float)
    num_frames = Column(Integer)
    is_short = Column(Boolean)


class VideosTranscribed(Base):
    __tablename__ = videos_transcribed

    video_id = Column(String, primary_key=True)
    transcribed_at = Column(TIMESTAMP)


class VideosSegmented(Base):
    __tablename__ = videos_segmented

    video_id = Column(String, primary_key=True)
    segmented_at = Column(TIMESTAMP)

class VideosEmbedded(Base):
    __tablename__ = videos_embedded

    video_id = Column(String, primary_key=True)
    embedded_at = Column(TIMESTAMP)


class VideosVectorStored(Base):
    __tablename__ = videos_vectors_stored

    video_id = Column(String, primary_key=True)
    uploaded_at = Column(TIMESTAMP)

class VideosEvaluated(Base):
    __tablename__ = videos_evaluated

    video_id = Column(String, primary_key=True)
    score = Column(Integer)
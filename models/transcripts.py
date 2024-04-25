from database.production_db.database import Base
from sqlalchemy import Column, String, Integer, Float
from table_names import transcripts_raw, transcripts_segmented


class TranscriptRaw(Base):
    __tablename__ = transcripts_raw

    transcript_id = Column(String, primary_key=True)
    time_start_ms = Column(Integer)
    d_duration_ms = Column(Integer)
    segs = Column(String)
    key = Column(String)
    video_id = Column(String)
    language = Column(String)


class TranscriptSegmented(Base):
    __tablename__ = transcripts_segmented

    segment_id = Column(String, primary_key=True)
    earliest_start_time = Column(Integer)
    latest_end_time = Column(Float)
    start_index = Column(Integer)
    end_index = Column(Integer)
    video_id = Column(String)
    index = Column(Integer)
    words = Column(String)
    transcript = Column(String)

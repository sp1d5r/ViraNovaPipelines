from database.production_db.database import Base
from sqlalchemy import Column, String, Integer
from table_names import transcripts_raw


class TranscriptRaw(Base):
    __tablename__ = transcripts_raw

    transcript_id = Column(String, primary_key=True)
    time_start_ms = Column(Integer)
    d_duration_ms = Column(Integer)
    segs = Column(String)
    key = Column(String)
    video_id = Column(String)
    language = Column(String)

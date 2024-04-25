from sqlalchemy import Column, Integer, Binary
from database.production_db.database import Base
from table_names import embeddings_segment, embeddings_transcript

class EmbeddingsSegment(Base):
    __tablename__ = embeddings_segment

    segment_id = Column(Integer, primary_key=True)
    embedding = Column(Binary)


class EmbeddingsTranscript(Base):
    __tablename__ = embeddings_transcript

    video_id = Column(Integer, primary_key=True)
    embedding = Column(Binary)
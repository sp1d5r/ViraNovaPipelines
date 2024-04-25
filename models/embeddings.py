from sqlalchemy import Column, String, LargeBinary
from database.production_db.database import Base
from table_names import embeddings_segment, embeddings_transcript


class EmbeddingsSegment(Base):
    __tablename__ = embeddings_segment

    segment_id = Column(String, primary_key=True)
    video_id = Column(String)
    embedding = Column(LargeBinary)


class EmbeddingsTranscript(Base):
    __tablename__ = embeddings_transcript

    video_id = Column(String, primary_key=True)
    embedding = Column(LargeBinary)

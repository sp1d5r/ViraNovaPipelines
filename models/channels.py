from database.production_db.database import Base
from sqlalchemy import Column, String, Boolean, TIMESTAMP
from table_names import channels_raw, channels_downloaded


class ChannelsRaw(Base):
    __tablename__ = channels_raw

    channel_link = Column(String)
    channel_id = Column(String, primary_key=True)
    channel_name = Column(String)


class ChannelsDownloaded(Base):
    __tablename__ = channels_downloaded

    channel_id = Column(String, primary_key=True)
    channel_link = Column(String)
    channel_name = Column(String)
    downloaded = Column(Boolean)
    date_added = Column(TIMESTAMP)

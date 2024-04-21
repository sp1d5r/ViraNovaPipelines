import pandas as pd
from database.production_database import ProductionDatabase
from table_names import channels_raw
from prefect import flow, get_run_logger, task

"""
Track Channels 
"""


@flow
def track_channel(channel_link: str = "UNDEFINED", channel_id: str = "UNDEFINED"):
    database = ProductionDatabase()
    channel_row = {"channel_link": [channel_link], "channel_id": [channel_id], "channel_name": [channel_link.split("/")[-1][1:]]}
    df = pd.DataFrame(channel_row)
    database.append_rows(df, channels_raw)


if __name__ == "__main__":
    track_channel.serve(
        name="Add a Channel to Track (Channels)",
        tags=["Ingestion", "Channels"],
        interval=60 * 24 * 15,
    )

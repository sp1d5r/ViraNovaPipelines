import os
import pandas as pd
from database.production_database import ProductionDatabase
from table_names import channels_downloaded, channels_raw, videos_raw
from dotenv import load_dotenv
from datetime import datetime
from services.youtube_data_collector import YoutubeDataCollector
from prefect import flow, get_run_logger, task

load_dotenv()

@task
def download_videos_from_channel(channel_id, max_total_results):
    yt_collector = YoutubeDataCollector(os.getenv("YOUTUBE_API"))
    return yt_collector.fetch_all_videos_from_channel(
        channel_id=channel_id,
        max_total_results=max_total_results
    )


"""

Extract Channel Videos:
- Retrieves a list of all channel videos form a youtube channel. Records the data these videos were extracted

"""
@flow
def extract_channel_videos(channel_limit: int = 3, max_total_results: int = 500):
    database = ProductionDatabase()
    logger = get_run_logger()

    if not database.table_exists(channels_downloaded):
        raise Exception("Channels Downloaded Table Does Not Exist.")

    if not database.table_exists(channels_raw):
        raise Exception("Channels RAW Table Does Not Exist.")

    # Load Tables
    channels_downloaded_df = database.read_table(channels_downloaded)
    channels_raw_df = database.read_table(channels_raw)

    # Update channels downloaded to include new all raw channels
    extended_channels_downloaded_df = channels_raw_df.merge(channels_downloaded_df[['channel_id', 'downloaded']], on='channel_id', how='left')
    extended_channels_downloaded_df['downloaded'] = extended_channels_downloaded_df['downloaded'].fillna(False)
    if "date_added" not in extended_channels_downloaded_df:
        extended_channels_downloaded_df['date_added'] = None

    # Prepare array for video information
    downloaded_videos = []

    # Ensure we only track 3 elements in dataframe
    count = 0

    # Loop through channels to download videos from
    for index, row in extended_channels_downloaded_df.iterrows():
        # If downloaded continue to next row
        # if not row['downloaded']:
        #     logger.info(f"Channel {row['channel_id']}: Already parsed")
        #     continue

        # If count exceeds the max results then break out of loop
        if count == channel_limit:
            break

        try:
            # Fetch videos from channels not yet downloaded
            collected_youtube_videos = download_videos_from_channel(row['channel_id'], max_total_results)
            downloaded_videos += collected_youtube_videos


            # Mark the channel as downloaded in channels_downloaded
            if len(collected_youtube_videos) >= 0:
                extended_channels_downloaded_df.at[index, 'downloaded'] = True
                extended_channels_downloaded_df.at[index, 'date_added'] = datetime.now()
            else:
                print(f"Error zero videos collected from youtube parse {row['channel_id']}")
                continue

        except Exception as e:
            logger.error(f"Error collecting videos for channel {row['channel_id']}: {e}")
            continue  # Continue to the next channel in case of error

        # Increment count
        count += 1

    # Update Database
    database.append_rows(pd.DataFrame(downloaded_videos), videos_raw)
    database.write_to_table(extended_channels_downloaded_df, channels_downloaded)



if __name__ == "__main__":
    extract_channel_videos.serve(
        name="Download Videos for Channel (Channels)",
        tags=["Ingestion", "Channels", "Videos"],
    )


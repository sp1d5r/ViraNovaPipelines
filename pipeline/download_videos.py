import os
import time
import pandas as pd
from database.production_database import ProductionDatabase
from table_names import videos_cleaned, videos_downloaded
from dotenv import load_dotenv
from pytube import YouTube
from prefect import flow, task, get_run_logger

load_dotenv()

VIDEO_DOWNLOAD_LOCATION = os.getenv("VIDEO_DOWNLOAD_LOCATION")
AUDIO_DOWNLOAD_LOCATION = os.getenv("AUDIO_DOWNLOAD_LOCATION")

@task
def download_video_from_id(video_id, logger):
    video_pathname = None
    audio_pathname = None
    yt = YouTube(f'https://www.youtube.com/watch?v={video_id}')
    if not yt:
        return None, None

    # Video Download
    try:
        video = yt.streams.filter(file_extension="mp4", resolution="480p")[0]
        video_pathname = VIDEO_DOWNLOAD_LOCATION + "video_"+ video_id + ".mp4"
        video.download(filename=video_pathname)
    except:
        logger.error(f"Failed to download video: {video_id}")

    # Audio Download
    try:
        audio = yt.streams.filter(file_extension="mp4", only_audio=True)[0]
        audio_pathname = AUDIO_DOWNLOAD_LOCATION + "audio_" + video_id + ".mp4"
        audio.download(filename=audio_pathname)
    except:
        logger.error(f"Failed to download audio: {video_id}")

    logger.info(f"Completed Download for video: {video_id}")

    return video_pathname, audio_pathname


"""
Download Videos
- Downloads videos specified in the videos_cleaned table
"""
@flow
def download_videos(max_downloads=20, sleep_time=10):
    database = ProductionDatabase()
    logger = get_run_logger()

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(videos_downloaded):
        raise Exception("Videos Downloaded Table Does Not Exist.")

    # Load Tables
    videos_cleaned_df = database.read_table(videos_cleaned)
    videos_downloaded_df = database.read_table(videos_downloaded)

    downloaded_videos = set(videos_downloaded_df['video_id'])

    sessions_downloaded_videos = []

    # Set a counter to ensure we are within the limit
    downloaded_videos_count = 0

    for index, video_cleaned in videos_cleaned_df.iterrows():
        # If we've already attempted a download then continue
        if video_cleaned['video_id'] in downloaded_videos:
            continue

        if downloaded_videos_count >= max_downloads:
            logger.info("Video Download Session Complete")
            break

        video_id = video_cleaned['video_id']
        logger.info(f"Attempting video download: {video_id}")

        video_path, audio_path = download_video_from_id(video_id, logger)

        downloaded_video = {
            'video_id': video_id,
            'video_downloaded': bool(video_path),
            'audio_downloaded': bool(audio_path),
            'video_downloaded_path': video_path,
            'audio_downloaded_path': audio_path,
        }

        sessions_downloaded_videos.append(downloaded_video)

        # Increment counter
        downloaded_videos_count += 1

        # Sleep to give rate limit
        time.sleep(sleep_time)

    # Update Database
    database.append_rows(pd.DataFrame(sessions_downloaded_videos), videos_downloaded)



if __name__ == "__main__":
    download_videos.serve(
        name="Download Videos (Downloading)",
        tags=["Data Ingestion", "Videos"],
    )

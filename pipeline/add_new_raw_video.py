import pandas as pd
from prefect import flow, get_run_logger

from services.youtube_data_collector import YoutubeDataCollector
from table_names import videos_raw
from database.production_database import ProductionDatabase
from dotenv import load_dotenv
import os

load_dotenv()

@flow
def add_new_raw_video(video_url: str = "UNDEFINED"):
    logger = get_run_logger()

    if video_url == "UNDEFINED":
        logger.error("No Video URL specified")
        return

    database = ProductionDatabase()
    yt_collector = YoutubeDataCollector(os.getenv("YOUTUBE_API"))

    if not database.table_exists(videos_raw):
        raise Exception("Videos Raw Table Does Not Exist.")

    logger.info("Extracting video id from the url.")
    video_id = yt_collector.get_video_id_from_url(video_url)
    logger.info(f"Extract video id: {video_id}, now collecting channel information")
    collected_video = yt_collector.fetch_video_info_from_video_id(video_id)

    print(collected_video)
    logger.info(f"Collected video info: {collected_video}")

    if collected_video:
        database.append_rows(pd.DataFrame([collected_video]), videos_raw)


if __name__ == "__main__":
    add_new_raw_video.serve(
        name="Add single video to tables.",
        tags=["Ingestion", "Videos"],
    )
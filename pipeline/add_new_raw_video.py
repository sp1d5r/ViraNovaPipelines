from prefect import flow, get_run_logger
from table_names import videos_raw
from database.production_database import ProductionDatabase


@flow
def add_new_raw_video(video_url: str = "UNDEFINED"):
    logger = get_run_logger()

    if video_url == "UNDEFINED":
        logger.error("No Video URL specified")
        return

    database = ProductionDatabase()

    if not database.table_exists(videos_raw):
        raise Exception("Videos Raw Table Does Not Exist.")
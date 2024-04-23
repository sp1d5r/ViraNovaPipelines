import os
import pandas as pd
from database.production_database import ProductionDatabase
from table_names import videos_downloaded, videos_type
import cv2
from dotenv import load_dotenv

load_dotenv()

VIDEO_DOWNLOAD_LOCATION = os.getenv("VIDEO_DOWNLOAD_LOCATION")


def analyse_video(video_id, video_path):
    # Open the video file
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return None

    # Get the number of frames
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Get the frame dimensions
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    # Calculate aspect ratio
    aspect_ratio = width / height

    cap.release()

    # Return the results
    return {
        "video_id": video_id,
        "aspect_ratio": aspect_ratio,
        "num_frames": num_frames,
        "is_short": aspect_ratio < 0.7
    }


"""
Determine Video Type
- Determines if the video is a short or an original
"""
def determine_video_type():
    database = ProductionDatabase()

    if not database.table_exists(videos_downloaded):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(videos_type):
        raise Exception("Videos Type Table Does Not Exist.")

    # Load Tables
    videos_downloaded_df = database.read_table(videos_downloaded)
    videos_type_df = database.read_table(videos_type)

    # Initialise Empty video array
    videos_determined = set(list(videos_type_df['video_id']))
    new_videos_type = []

    for index, row in videos_downloaded_df.iterrows():
        # Check if the video type has already been calculated
        if row['video_id'] in videos_determined:
            continue

        print(f"Analysing video: {row['video_id']}")

        analysed_video = analyse_video(row['video_id'], row['video_downloaded_path'])

        if analysed_video:
            new_videos_type.append(analysed_video)

    # Update Database
    database.append_rows(pd.DataFrame(new_videos_type), videos_type)

if __name__ == "__main__":
    determine_video_type()

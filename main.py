from prefect import serve
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule

# Pipelines
from pipeline.track_channels import track_channel
from pipeline.clean_videos import clean_raw_videos
from pipeline.download_videos import download_videos
from pipeline.determine_video_type import determine_video_type
from pipeline.extract_channel_videos import extract_channel_videos
from pipeline.extract_transcript import download_video_transcript
from pipeline.add_new_raw_video import add_new_raw_video
from pipeline.performing_topical_segmentation import perform_topical_segmentation
from pipeline.get_transcript_embeddings import get_transcript_embeddings
from pipeline.get_segment_embeddings import get_segment_embeddings
from pipeline.store_to_vector_db import store_shorts_to_vector_db, store_segments_to_vector_db

"""
    PREFECT ENTRY POINT

    In this file, we're deploying all the pipelines created in prefect. This is the entry point
    where we store all of the prefect deployments.
"""

# Execute the main flow
if __name__ == "__main__":
    # Tracking a YouTube channel to extract all the videos from it.
    track_channel_deployment = track_channel.to_deployment(
        name="Add a Channel to Track [Channels]",
        tags=["Ingestion", "Channels"],
    )

    # Extracting all the videos from a YouTube channel (YouTube API)
    extract_channel_videos_deployment = extract_channel_videos.to_deployment(
        name="Extract Videos for Channel [Channels]",
        tags=["Ingestion", "Channels", "Videos"],
        schedule=IntervalSchedule(
            interval=timedelta(hours=6),
            anchor_date=datetime(2023, 1, 1, 0, 0),
            timezone="America/Chicago"
        )
    )

    # Extract a single video ID's video information
    add_new_raw_video_deployment = add_new_raw_video.to_deployment(
        name="Extract Single Video Information  [Videos]",
        tags=["Ingestion", "Videos", "Transcripts"],
    )

    # Cleaning the raw videos
    clean_raw_videos_deployment = clean_raw_videos.to_deployment(
        name="Clean Videos [Videos]",
        tags=["Data Cleaning", "Videos"],
    )

    # Download videos to download location
    download_videos_deployment = download_videos.to_deployment(
        name="Download Videos [Downloading]",
        tags=["Data Ingestion", "Videos"],
    )

    # Download the transcripts for youtube videos
    download_transcript_deployment = download_video_transcript.to_deployment(
        name="Download Video Transcripts [Transcripts]",
        tags=["Ingestion", "Videos", "Transcripts"],
        schedule=IntervalSchedule(
            interval=timedelta(hours=50),
            anchor_date=datetime(2023, 1, 1, 0, 0),
            timezone="America/Chicago"
        )
    )

    # Extract video type
    determine_video_type_deployment = determine_video_type.to_deployment(
        name="Determine Video Type [Videos]",
        tags=["Analysis", "Videos"],
    )

    # Perform Topical Segmentation
    perform_topical_segmentation_deployment = perform_topical_segmentation.to_deployment(
        name="Perform Topical Segmentation [Transcripts]",
        tags=["Analysis", "Transcripts"],
        schedule=IntervalSchedule(
            interval=timedelta(hours=1),
            anchor_date=datetime(2023, 1, 1, 0, 30),
            timezone="America/Chicago"
        )
    )

    # Embedding Transcripts
    get_transcript_embeddings_deployment = get_transcript_embeddings.to_deployment(
        name="Embed Transcripts [Embeddings]",
        tags=["Analysis", "Embeddings"],
        schedule=IntervalSchedule(
            interval=timedelta(minutes=10),
            anchor_date=datetime(2023, 1, 1, 1, 0),
            timezone="America/Chicago"
        )
    )

    # Embedding Segments
    get_segment_embeddings_deployment = get_segment_embeddings.to_deployment(
        name="Embed Segments [Embeddings]",
        tags=["Analysis", "Embeddings"],
        schedule=IntervalSchedule(
            interval=timedelta(minutes=10),
            anchor_date=datetime(2023, 1, 1, 0, 0),
            timezone="America/Chicago"
        )
    )

    store_segments_to_vector_db_deployment = store_segments_to_vector_db.to_deployment(
        name="Store Segments to Vector DB [VectorDB]",
        tags=["Upload", "Embeddings"],
        schedule=IntervalSchedule(
            interval=timedelta(hours=4),
            anchor_date=datetime(2023, 1, 1, 0, 17),
            timezone="America/Chicago"
        )
    )

    # Storing Segments
    store_shorts_to_vector_db_deployment = store_shorts_to_vector_db.to_deployment(
        name="Store Shorts to Vector DB [VectorDB]",
        tags=["Upload", "Embeddings"],
        schedule=IntervalSchedule(
            interval=timedelta(hours=4),
            anchor_date=datetime(2023, 1, 1, 0, 25),
            timezone="America/Chicago"
        )
    )

    serve(
        track_channel_deployment,
        extract_channel_videos_deployment,
        add_new_raw_video_deployment,
        clean_raw_videos_deployment,
        download_videos_deployment,
        download_transcript_deployment,
        determine_video_type_deployment,
        perform_topical_segmentation_deployment,
        get_transcript_embeddings_deployment,
        get_segment_embeddings_deployment,
        store_segments_to_vector_db_deployment,
        store_shorts_to_vector_db_deployment
    )
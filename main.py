from prefect import serve

from pipeline import determine_video_type
# Channel Parsing
from pipeline.track_channels import track_channel
from pipeline.clean_videos import clean_raw_videos
from pipeline.download_videos import download_videos
from pipeline.extract_channel_videos import extract_channel_videos
from pipeline.extract_transcript import download_video_transcript
from pipeline.add_new_raw_video import add_new_raw_video

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
    )

    # Extract video type
    determine_video_type_deployment = determine_video_type.to_deployment(
        name="Determine Video Type [Videos]",
        tags=["Analysis", "Videos"],
    )

    serve(
        track_channel_deployment,
        extract_channel_videos_deployment,
        add_new_raw_video_deployment,
        clean_raw_videos_deployment,
        download_videos_deployment,
        download_transcript_deployment,
        determine_video_type_deployment,

    )
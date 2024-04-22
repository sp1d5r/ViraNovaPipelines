from prefect import serve

# Channel Parsing
from pipeline.track_channels import track_channel
from pipeline.clean_videos import clean_raw_videos
from pipeline.download_videos import download_videos
from pipeline.extract_channel_videos import extract_channel_videos


"""
    PREFECT ENTRY POINT

    In this file, we're deploying all the pipelines created in prefect. This is the entry point
    where we store all of the prefect deployments.
"""

# Execute the main flow
if __name__ == "__main__":
    track_channel_deployment = track_channel.to_deployment(
        name="Add a Channel to Track (Channels)",
        tags=["Ingestion", "Channels"],
    )

    extract_channel_videos_deployment = extract_channel_videos.to_deployment(
        name="Download Videos for Channel (Channels)",
        tags=["Ingestion", "Channels", "Videos"],
    )

    clean_raw_videos_deployment = clean_raw_videos.to_deployment(
        name="Clean Videos (Videos)",
        tags=["Data Cleaning", "Videos"],
    )

    download_videos_deployment = download_videos.to_deployment(
        name="Download Videos (Downloading)",
        tags=["Data Ingestion", "Videos"],
    )

    serve(
        track_channel_deployment,
        extract_channel_videos_deployment,
        clean_raw_videos_deployment,
        download_videos_deployment
    )
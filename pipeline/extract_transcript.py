from datetime import datetime
from prefect import flow, task, get_run_logger
import pandas as pd
from database.production_database import ProductionDatabase
from table_names import videos_cleaned, videos_transcribed, transcripts_raw
from pytube import YouTube


def clean_captions(video_id, caption, key):
    print("captions")
    events = caption['events']
    sorted_events = sorted(events, key=lambda x: x['tStartMs'])
    cleaned_events = [
        {
            'transcript_id': video_id + "_" + str(index),
            'time_start_ms': item['tStartMs'],
            'd_duration_ms': item['dDurationMs'],
            'segs': str(item['segs']),
            'key': index,
            'video_id': video_id,
            'language': key
        }
        for index, item in enumerate(sorted_events) if 'segs' in item and 'dDurationMs' in item and 'tStartMs' in item
    ]
    # convert this to pandas
    cleaned_captions = pd.DataFrame(cleaned_events)
    return cleaned_captions


@task
def download_transcript_from_video_id(video_id, logger):
    yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
    try:
        streams = yt.streams
        yt.bypass_age_gate()
        captions = yt.captions
        if not captions:
            logger.info("No Captions Available for Video")
            return None

        if "en" in captions:
            logger.info("Extracting English Captions")
            retrieved_captions = captions.get("en")
            retrieved_captions_json = retrieved_captions.json_captions
            return clean_captions(video_id, retrieved_captions_json, key="en")

        logger.info("Extracting Auto Generated Captions")
        logger.info(captions)
        retrieved_captions = captions.get("a.en")
        retrieved_captions_json = retrieved_captions.json_captions
        return clean_captions(video_id, retrieved_captions_json, key="a.en")
    except Exception as e:
        logger.info(f"Error in Downloading Transcript: {e}")
        return None


"""
Download Video Transcript
- Determines if the video is a short or an original
"""

@flow
def download_video_transcript(max_downloads: int = 50):
    database = ProductionDatabase()
    logger = get_run_logger()

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(transcripts_raw):
        raise Exception("Transcripts Raw Table Does Not Exist.")

    if not database.table_exists(videos_transcribed):
        raise Exception("Videos Transcribed Table Does Not Exist.")

    # Load Tables
    videos_cleaned_df = database.read_table(videos_cleaned)
    videos_transcribed_df = database.read_table(videos_transcribed)

    # Get a list of all transcribed videos
    videos_transcribed_list = set(list(videos_transcribed_df['video_id']))

    # Initialise Empty video array
    new_transcriptions = []
    transcription_log = []

    count = 0

    for index, row in videos_cleaned_df.iterrows():
        # Check if we've collected enough transcripts for now
        if count >= max_downloads:
            print(f"Finished collecting {max_downloads} transcripts.")
            break

        # Check if the video type has already been transcribed
        if row['video_id'] in videos_transcribed_list:
            continue

        print(f"Extracting transcript for {row['video_id']}")
        transcription = download_transcript_from_video_id(row['video_id'], logger)

        if transcription is not None:
            new_transcriptions.append(transcription)
            transcription_log.append({'video_id': row['video_id'], 'transcribed_at': datetime.now()})
            count += 1

    # Update Database
    if new_transcriptions:
        # Concatenate all DataFrame objects in the list
        all_transcriptions_df = pd.concat(new_transcriptions, ignore_index=True)
        database.append_rows(all_transcriptions_df, transcripts_raw)
        database.append_rows(pd.DataFrame(transcription_log), videos_transcribed)
    else:
        print("No new transcriptions to update.")


if __name__ == "__main__":
    download_video_transcript.serve(
        name="Download Video Transcripts [Transcripts]",
        tags=["Ingestion", "Videos", "Transcripts"],
    )

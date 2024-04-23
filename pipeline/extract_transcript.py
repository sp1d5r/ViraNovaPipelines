from datetime import datetime

import pandas as pd
from database.production_database import ProductionDatabase
from table_names import videos_downloaded, videos_transcribed, transcripts_raw
from pytube import YouTube


def clean_captions(video_id, caption, key):
    print("captions")
    events = caption['events']
    cleaned_events = [
        {
            'transcript_id': video_id + "_" + index,
            'time_start_ms': item['tStartMs'],
            'd_duration_ms': item['dDurationMs'],
            'segs': str(item['segs']),
            'key': index,
            'video_id': video_id,
            'language': key
        }
        for index, item in enumerate(events) if 'segs' in item and 'dDurationMs' in item and 'tStartMs' in item
    ]
    # convert this to pandas
    cleaned_captions = pd.DataFrame(cleaned_events)
    return cleaned_captions


def download_transcript_from_video_id(video_id):
    yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
    try:
        streams = yt.streams
        yt.bypass_age_gate()
        captions = yt.captions
        if not captions:
            print("No Captions Available for Video")
            return None

        if "en" in captions:
            print("Extracting English Captions")
            retrieved_captions = captions.get("en")
            retrieved_captions_json = retrieved_captions.json_captions
            return clean_captions(video_id, retrieved_captions_json, key="en")

        print("Extracting Auto Generated Captions")
        print(captions)
        retrieved_captions = captions.get("a.en")
        retrieved_captions_json = retrieved_captions.json_captions
        return clean_captions(video_id, retrieved_captions_json, key="a.en")
    except:
        print("Error in Downloading Transcript")
        return None


"""
Download Video Transcript
- Determines if the video is a short or an original
"""


def download_video_transcript(max_downloads: int = 50):
    database = ProductionDatabase()

    if not database.table_exists(videos_downloaded):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(transcripts_raw):
        raise Exception("Transcripts Raw Table Does Not Exist.")

    if not database.table_exists(videos_transcribed):
        raise Exception("Videos Transcribed Table Does Not Exist.")

    # Load Tables
    videos_downloaded_df = database.read_table(videos_downloaded)
    transcripts_raw_df = database.read_table(transcripts_raw)
    videos_transcribed_df = database.read_table(videos_transcribed)

    # Get a list of all transcribed videos
    videos_transcribed_list = set(list(videos_transcribed_df['video_id']))

    # Initialise Empty video array
    new_transcriptions = []
    transcription_log = []

    count = 0

    for index, row in videos_downloaded_df.iterrows():
        # Check if we've collected enough transcripts for now
        if count >= max_downloads:
            print(f"Finished collecting {max_downloads} transcripts.")
            break

        # Check if the video type has already been transcribed
        if row['video_id'] in videos_transcribed_list:
            continue

        print(f"Extracting transcript for {row['video_id']}")
        transcription = download_transcript_from_video_id(row['video_id'])

        if transcription is not None:
            new_transcriptions.append(transcription)
            transcription_log.append({'video_id': row['video_id'], 'transcription_date': datetime.now()})
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
    download_video_transcript()

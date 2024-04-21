import pandas as pd
from database.production_database import ProductionDatabase
from table_names import videos_downloaded, transcripts_raw
from pytube import YouTube

def clean_captions(video_id, caption, key):
    print("captions")
    events = caption['events']
    cleaned_events = [
        {
            'transcript_id': video_id +"_" + key,
            't_start_ms': item['tStartMs'],
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
Determine Video Type
- Determines if the video is a short or an original
"""
def download_video_transcript():
    database = ProductionDatabase()

    if not database.table_exists(videos_downloaded):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(transcripts_raw):
        raise Exception("Transcripts Raw Table Does Not Exist.")

    # Load Tables
    videos_downloaded_df = database.read_table(videos_downloaded)
    transcripts_raw_df = database.read_table(transcripts_raw)

    # Initialise Empty video array
    videos_transcribed = set(list(transcripts_raw_df['videoId']))
    new_transcriptions = []

    for index, row in videos_downloaded_df:
        # Check if the video type has already been transcribed
        if row['videoId'] in videos_transcribed:
            continue

        transcription = download_transcript_from_video_id(row['videoId'])
        new_transcriptions.append(transcription)

    # Update Database
    database.append_rows(pd.DataFrame(new_transcriptions), transcripts_raw)


if __name__ == "__main__":
    download_video_transcript()

from pytube import YouTube
import pandas as pd
import ast

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

def download_transcript_from_video_id(video_id, log):
    yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
    try:
        streams = yt.streams
        yt.bypass_age_gate()
        captions = yt.captions
        if not captions:
            log("No Captions Available for Video")
            return None

        if "en" in captions:
            log("Extracting English Captions")
            retrieved_captions = captions.get("en")
            retrieved_captions_json = retrieved_captions.json_captions
            return clean_captions(video_id, retrieved_captions_json, key="en")

        log("Extracting Auto Generated Captions")
        log(captions)
        retrieved_captions = captions.get("a.en")
        retrieved_captions_json = retrieved_captions.json_captions
        return clean_captions(video_id, retrieved_captions_json, key="a.en")
    except Exception as e:
        log(f"Error in Downloading Transcript: {e}")
        return None

def from_transcript_df_extract_text(transcription_df):
    return ' '.join(list(transcription_df['segs'].apply(lambda x: ' '.join([i['utf8'].strip() for i in ast.literal_eval(x)]))))


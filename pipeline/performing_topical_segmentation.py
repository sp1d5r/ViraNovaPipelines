from datetime import datetime
import time
import numpy as np
import ast
import json
import pandas as pd
from prefect import flow, get_run_logger
from services.open_ai import OpenAIService
from database.production_database import ProductionDatabase
from table_names import transcripts_raw, transcripts_segmented, videos_transcribed, videos_segmented, videos_cleaned

# Variables to stay within context limit open AI one request
TOTAL_AVAILABLE_TOKENS = 8000  # Lower Bound
TOKEN_SIZE = 3
AVERAGE_ENGLISH_WORD_LENGTH = 8
FIXED_SEGMENT_LENGTH = 30
AVERAGE_CHAR_SEGMENT = FIXED_SEGMENT_LENGTH * AVERAGE_ENGLISH_WORD_LENGTH
SEGMENTS_TO_SEND_IN_PARALLEL = int((TOTAL_AVAILABLE_TOKENS * TOKEN_SIZE) / AVERAGE_CHAR_SEGMENT)


def create_fixed_length_transcripts(transcripts, n=100):
    fixed_length_transcripts = []

    # Ensure transcripts are sorted by a key
    transcripts = transcripts.sort_values(by='key')
    for _, transcript in transcripts.iterrows():
        tStartMs = transcript['time_start_ms']
        window_words = []
        segment_index = 0

        # Try to evaluate the segments from string format
        try:
            segs = ast.literal_eval(transcript['segs'])
        except ValueError as e:
            print(f"Error parsing segs for video_id {transcript['video_id']}: {e}")
            continue

        for seg in segs:
            word = seg['utf8'].strip()
            if word == '':
                continue
            offset_ms = seg.get('tOffsetMs', 0)  # Default to 0 if tOffsetMs is not available
            start_time = tStartMs + offset_ms

            # Create a dictionary for each word with word and start time
            word_info = {
                'word': word,
                'start_time': start_time
            }

            window_words.append(word_info)
            segment_index += 1

            # Check if the window is full or if it's the last word in the segment
            if len(window_words) == n or segment_index == len(segs):
                first_word_time = window_words[0]['start_time']
                # Estimate end time from the last word in the window by adding average word duration
                end_time = window_words[-1]['start_time'] + (tStartMs / len(segs))

                # Compile the window words into a transcript text
                transcript_text = " ".join([w['word'] for w in window_words])

                # JSON-format the word details
                words_json = json.dumps(window_words)

                # Append the fixed-length transcript segment to the list
                fixed_length_transcripts.append({
                    'start_time': first_word_time,
                    'end_time': end_time,
                    'transcript': transcript_text,
                    'words': words_json  # Store word-level information as JSON string
                })

                # Reset the window for the next segment
                window_words = []

    return fixed_length_transcripts

def extract_boundaries(angular_distances, threshold):
    # Convert angular distances to a binary segmentation based on the threshold
    binary_segmentation = [1 if distance > threshold else 0 for distance in angular_distances]
    return binary_segmentation

def cosine_similarity(v1, v2):
    """Compute the cosine similarity between two vectors."""
    # Normalize the vectors to have unit length
    v1_normalized = v1 / np.linalg.norm(v1)
    v2_normalized = v2 / np.linalg.norm(v2)
    # Compute the cosine similarity
    return np.dot(v1_normalized, v2_normalized)


def angular_distance(similarity):
    """Convert cosine similarity to angular distance in degrees."""
    # Clip the similarity to ensure it's within the valid range for arccos
    similarity_clipped = np.clip(similarity, -1, 1)
    try:
        # Compute the angular distance in radians
        angle_rad = np.arccos(similarity_clipped)
        # Convert to degrees for easier interpretation
        angle_deg = np.degrees(angle_rad)
        return angle_deg
    except Exception as e:
        print(f"Error computing angular distance: {e}")
        return 0


def calculate_boundaries_for_segments(subset_embeddings, update_progress):
    # Optimal Values for this video where:
    # Threshold = mean + 1.05 * std
    std = 1.05

    # Compute angular distance between consecutive embeddings
    angular_distances = []

    for i in range(len(subset_embeddings) - 1):
        update_progress(i / (len(subset_embeddings) - 1) * 100)
        sim = cosine_similarity(subset_embeddings[i], subset_embeddings[i + 1])
        ang_dist = angular_distance(sim)

        # Check if ang_dist is NaN, and if so, append None
        if np.isnan(ang_dist):
            angular_distances.append(None)
        elif ang_dist == 0:
            angular_distances.append(None)
        else:
            angular_distances.append(ang_dist)

    # Now calculate the mean from the valid angular distances only
    valid_distances = [d for d in angular_distances if d is not None]

    if valid_distances:
        mean_angular_distance = np.mean(valid_distances)
        std_deviation = np.std(valid_distances)
    else:
        # Handle the case where valid_distances is empty
        mean_angular_distance = 0
        std_deviation = 0

    # Replace None values with the mean angular distance
    angular_distances = [d if d is not None else mean_angular_distance for d in angular_distances]

    return extract_boundaries(angular_distances, mean_angular_distance + std_deviation * std)

def get_transcript_topic_boundaries(embeddings, update_progress, update_progress_message):
    update_progress_message("Extracting the Transcript Boundaries")
    boundaries = calculate_boundaries_for_segments(embeddings, update_progress)
    return boundaries


def create_segments(fixed_length_transcripts, boundaries, video_id, update_progress, update_progress_message):
    segments = []
    current_segment_transcripts = []
    current_segment_words = []
    start_index = 0
    earliest_start_time = None
    latest_end_time = None

    update_progress_message("Creating new segments")

    for i, (transcript, boundary) in enumerate(zip(fixed_length_transcripts, boundaries)):
        # Initialize for the first segment or new segment
        update_progress(i / (len(fixed_length_transcripts) - 1) * 100)

        if boundary == 1 or i == 0:
            if current_segment_transcripts:
                # Compile words from all transcripts in the current segment
                compiled_words_json = json.dumps(current_segment_words)

                # Save the previous segment
                segment = {
                    'earliest_start_time': earliest_start_time,
                    'latest_end_time': latest_end_time,
                    'start_index': start_index,
                    'end_index': i - 1,
                    'video_id': video_id,
                    'index': len(segments),
                    'transcript': " ".join(current_segment_transcripts),
                    'words': compiled_words_json,
                    'segment_id': video_id + '_' + str(len(segments))
                }
                segments.append(segment)
                current_segment_transcripts = []
                current_segment_words = []

            # Reset for new segment
            start_index = i
            earliest_start_time = transcript['start_time']
            latest_end_time = transcript['end_time']
            current_segment_transcripts.append(transcript['transcript'])
            # Append word-level details for the new segment
            current_segment_words.extend(json.loads(transcript['words']))
        else:
            # Continue with the current segment
            latest_end_time = max(latest_end_time, transcript['end_time'])
            current_segment_transcripts.append(transcript['transcript'])
            current_segment_words.extend(json.loads(transcript['words']))

    # Add the last segment if there are remaining transcripts
    if current_segment_transcripts:
        compiled_words_json = json.dumps(current_segment_words)
        segment = {
            'earliest_start_time': earliest_start_time,
            'latest_end_time': latest_end_time,
            'start_index': start_index,
            'end_index': len(fixed_length_transcripts) - 1,
            'video_id': video_id,
            'index': len(segments),
            'transcript': " ".join(current_segment_transcripts),
            'words': compiled_words_json,
            'segment_id': video_id + '_' + str(len(segments))
        }
        segments.append(segment)

    return segments

@flow
def perform_topical_segmentation(number_of_videos_to_segment: int = 15):
    database = ProductionDatabase()
    open_ai_service = OpenAIService()
    logger = get_run_logger()
    update_progress = lambda x: print('Progress : ' + str(x))
    progress_message = lambda x: print("Progress Message: " + str(x))

    if not database.table_exists(transcripts_raw):
        raise Exception("Transcripts Raw Table Does Not Exist.")

    if not database.table_exists(videos_transcribed):
        raise Exception("Videos Transcribed Table Does Not Exist.")

    if not database.table_exists(videos_segmented):
        raise Exception("Videos Segmented Tracker Table Does Not Exist.")

    if not database.table_exists(transcripts_segmented):
        raise Exception("Transcripts Segmented Tracker Table Does Not Exist.")

    # Load in tables to memory
    videos_transcribed_df = database.read_table(videos_transcribed)
    videos_cleaned_df = database.read_table(videos_cleaned)
    videos_segmented_df = database.read_table(videos_segmented)

    # Videos downloaded
    all_videos_segmented = set(videos_segmented_df['video_id'])
    original_videos = set(videos_cleaned_df[videos_cleaned_df['duration'] > 60 * 1000]['video_id'])

    print(len(original_videos))
    # Eligible original videos to segment
    eligible_videos_df = videos_transcribed_df[~videos_transcribed_df['video_id'].isin(all_videos_segmented)]
    eligible_videos_df = eligible_videos_df[eligible_videos_df['video_id'].isin(original_videos)]

    # number_to_segment = min(number_of_videos_to_segment, len(eligible_videos_df))
    selected_videos_df = eligible_videos_df

    # Tracking the count
    count = 0

    for index, video_row in selected_videos_df.iterrows():
        video_id = video_row['video_id']
        logger.info(f"Performing topical segmentation for video {video_id}")

        if count >= number_of_videos_to_segment:
            logger.info(f"Completed topical segmentation for {number_of_videos_to_segment} videos.")
            break

        transcripts_extracted = database.query_table_by_column(transcripts_raw, 'video_id', video_id)
        fixed_length_transcripts = create_fixed_length_transcripts(transcripts_extracted, n=FIXED_SEGMENT_LENGTH)

        transcripts_embeddings = open_ai_service.get_embeddings_parallel(
            transcripts=fixed_length_transcripts,
            batch_size=SEGMENTS_TO_SEND_IN_PARALLEL,
            update_progress=update_progress)
        transcript_boundaries = get_transcript_topic_boundaries(transcripts_embeddings, update_progress, progress_message)
        topical_segments = create_segments(fixed_length_transcripts, transcript_boundaries, video_id,
                                            update_progress, progress_message)

        database.append_rows(pd.DataFrame([{'video_id': video_id, 'segmented_at': datetime.now()}]), videos_segmented)
        database.append_rows(pd.DataFrame(topical_segments), transcripts_segmented)

        time.sleep(30)

        count += 1


if __name__=="__main__":
    perform_topical_segmentation.serve(
        name="Perform Topical Segmentation [Transcripts]",
        tags=["Analysis", "Transcripts"],
    )
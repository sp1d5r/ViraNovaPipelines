import ast
import time
from datetime import datetime
from prefect import flow, get_run_logger
import pandas as pd
from database.production_database import ProductionDatabase
from services.open_ai import OpenAIService
import numpy as np
import pickle
from table_names import videos_embedded, videos_transcribed, transcripts_raw,  videos_cleaned, embeddings_transcript


@flow
def get_transcript_embeddings(videos_to_embedd: int = 50):
    database = ProductionDatabase()
    open_ai_service = OpenAIService()
    logger = get_run_logger()

    if not database.table_exists(videos_embedded):
        raise Exception("Videos Embeddings Table Does Not Exist.")

    if not database.table_exists(videos_transcribed):
        raise Exception("Videos Transcribed Table Does Not Exist.")

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(transcripts_raw):
        raise Exception("Transcripts Raw Table Does Not Exist.")

    if not database.table_exists(embeddings_transcript):
        raise Exception("Embeddings Transcript Table Does Not Exist.")

    videos_embedded_df = database.read_table(videos_embedded)
    videos_transcribed_df = database.read_table(videos_transcribed)

    embedded_video_ids = set(videos_embedded_df['video_id'])
    videos_cleaned_df = database.read_table(videos_cleaned)
    short_videos = set(videos_cleaned_df[videos_cleaned_df['duration'] <= 5 * 60 * 1000]['video_id'])

    # Eligible original videos to embedd
    eligible_videos_df = videos_transcribed_df[~videos_transcribed_df['video_id'].isin(embedded_video_ids)]
    eligible_videos_df = eligible_videos_df[eligible_videos_df['video_id'].isin(short_videos)]

    # Tracking the count
    count = 0

    for index, video_row in eligible_videos_df.iterrows():
        video_id = video_row['video_id']

        if count >= videos_to_embedd:
            logger.info(f"Completed video embedding stage for {videos_to_embedd} videos.")
            break

        logger.info(f"Extracting transcript for video: {video_id}")
        transcripts = database.query_table_by_column(transcripts_raw, 'video_id', video_id)

        transcript_value = " ".join(transcripts['segs'].apply(lambda x: " ".join([i['utf8'].strip() for i in ast.literal_eval(x)])))
        logger.info(f"Calculating embedding for transcript")
        embedding = open_ai_service.get_single_embedding(transcript_value)

        embedding_array = np.array(embedding)
        serialized_embedding = pickle.dumps(embedding_array, protocol=pickle.HIGHEST_PROTOCOL)
        try:
            database.append_rows(pd.DataFrame([{
                'video_id': video_id,
                'embedding': serialized_embedding
            }]), embeddings_transcript)
        except Exception as e:
            logger.info(f"Failed to upload embedded transcript: {video_id}, {e}")
        try:
            database.append_rows(
                pd.DataFrame(
                    [{
                        'video_id': video_id,
                        'embedded_at': datetime.now()
                    }]
                ),
                videos_embedded
            )
        except Exception as e:
            logger.info(f"Failed to update transcript tracker: {video_id}, {e}")

        time.sleep(15)
        count += 1


if __name__ == "__main__":
    get_transcript_embeddings.serve(
        name="Embed Transcripts [Embeddings]",
        tags=["Analysis", "Embeddings"],
    )

import time
from datetime import datetime
from prefect import flow
import pandas as pd
from database.production_database import ProductionDatabase
from services.open_ai import OpenAIService
import numpy as np
import pickle
from table_names import transcripts_segment_embedded, transcripts_segmented, videos_segmented, embeddings_segment


@flow
def get_segment_embeddings(videos_to_embedd: int = 50):
    database = ProductionDatabase()
    open_ai_service = OpenAIService()

    if not database.table_exists(transcripts_segment_embedded):
        raise Exception("Transcript Segment Embeddings Table Does Not Exist.")

    if not database.table_exists(transcripts_segmented):
        raise Exception("Transcripts Segmented Tracker Table Does Not Exist.")

    if not database.table_exists(videos_segmented):
        raise Exception("Transcripts Segmented Tracker Table Does Not Exist.")

    if not database.table_exists(embeddings_segment):
        raise Exception("Embeddings Segment Tracker Table Does Not Exist.")

    segments_embedded_df = database.read_table(transcripts_segment_embedded)
    embedded_video_ids = set(segments_embedded_df['video_id'])
    videos_segmented_df = database.read_table(videos_segmented)

    eligible_video_df = videos_segmented_df[~videos_segmented_df['video_id'].isin(embedded_video_ids)]

    # Tracking the count
    count = 0

    for index, video_row in eligible_video_df.iterrows():
        video_id = video_row['video_id']

        if count >= videos_to_embedd:
            print(f"Completed video embedding stage for {videos_to_embedd} videos.")
            break

        print(f"Extracting segments for video: {video_id}")
        segments = database.query_table_by_column(transcripts_segmented, 'video_id', video_id)

        for seg_index, segment_row in segments.iterrows():
            segment_id = segment_row['segment_id']
            print(f"Calculating embedding for segment: {segment_id}")
            transcript_to_embed = segment_row['transcript']
            embedding = open_ai_service.get_single_embedding(transcript_to_embed)

            # Convert to binary format
            embedding_array = np.array(embedding)
            serialized_embedding = pickle.dumps(embedding_array, protocol=pickle.HIGHEST_PROTOCOL)
            try:
                database.append_rows(pd.DataFrame([{
                    'segment_id': segment_id,
                    'video_id': video_id,
                    'embedding': serialized_embedding
                }]), embeddings_segment)
            except Exception as e:
                print(f"Failed to upload embedded segment: {segment_id}, {e}")
            try:
                database.append_rows(
                    pd.DataFrame(
                        [{
                            'segment_id': segment_id,
                            'video_id': video_id,
                            'embedded_at': datetime.now()
                        }]
                    ),
                    transcripts_segment_embedded
                )
            except Exception as e:
                print(f"Failed to update segment embedding tracker: {segment_id}, {e}")

            time.sleep(15)

        count += 1


if __name__=="__main__":
    get_segment_embeddings.serve(
        name="Embed Segments [Embeddings]",
        tags=["Analysis", "Embeddings"],
    )
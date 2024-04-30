from datetime import datetime
import pandas as pd
from database.production_database import ProductionDatabase
from table_names import (
    videos_cleaned,
    embeddings_transcript,
    embeddings_segment,
    transcripts_segment_embedded,
    transcripts_segment_vectors_stored,
    videos_embedded,
    videos_vectors_stored
)
import pickle
import numpy as np
import uuid
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import os
from dotenv import load_dotenv
from prefect import flow, get_run_logger

load_dotenv()
NAMESPACE = uuid.NAMESPACE_DNS


class TestLog:
    def __init__(self):
        pass

    def info(self, x):
        print(x)

# @flow
def store_segments_to_vector_db(number_of_segments_to_upload: int = 50):
    database = ProductionDatabase()
    client = QdrantClient(url=os.getenv("QDRANT_LOCATION"))
    logger = TestLog() # get_run_logger()

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Not Found")

    if not database.table_exists(embeddings_segment):
        raise Exception("Embeddings Segments Does Not Exist")

    if not database.table_exists(transcripts_segment_embedded):
        raise Exception("Transcript Segment Embedding Tracker Does Not Exist")

    if not database.table_exists(transcripts_segment_vectors_stored):
        raise Exception("Transcript Segment Vectors Stored Tracker Does Not Exist")

    if not client.collection_exists("videos_and_segments"):
        raise Exception("(QDRANT) Collection videos_and_segments Does Not Exist")

    # Load in tables to memory
    videos_cleaned_df = database.read_table(videos_cleaned)
    transcripts_segment_embedded_df = database.read_table(transcripts_segment_embedded)
    transcripts_segment_vectors_stored_df = database.read_table(transcripts_segment_vectors_stored)

    # Determine eligible segments
    segments_already_stored = set() # set(transcripts_segment_vectors_stored_df['segment_id'])
    eligible_segments_df = transcripts_segment_embedded_df[~transcripts_segment_embedded_df['segment_id'].isin(segments_already_stored)]

    count = 0

    for index, segment_embedding in eligible_segments_df.iterrows():
        if count >= number_of_segments_to_upload:
            logger.info(f"Finished uploading {number_of_segments_to_upload} segments to vector db")
            break

        segment_id = segment_embedding['segment_id']
        video_id = segment_embedding['video_id']

        segment_rows = database.query_table_by_column(embeddings_segment, 'segment_id', segment_id)

        if len(segment_rows) < 1:
            logger.info(f"Unable to find entry for {segment_id} in embeddings_segment table.")
            continue

        # Get the embedding information
        embedding_entry = segment_rows.iloc[0]
        videos_cleaned_info = videos_cleaned_df[videos_cleaned_df['video_id'] == video_id]

        if len(videos_cleaned_info) < 1:
            logger.info(f"Unable to find entry for {video_id} in videos_cleaned table.")
            continue

        logger.info(f"Attempting to upload segment ({segment_id}) to vector db.")

        videos_cleaned_entry = videos_cleaned_info.iloc[0]

        # Extract original embedding
        serialized_embedding = embedding_entry['embedding']
        deserialized_embedding = pickle.loads(serialized_embedding)  # ~ np.array

        payload = {
            'video_id': video_id,
            'is_segment': True,
            'segment_id': segment_id,
            'channel_id': videos_cleaned_entry['channel_id']
        }

        vector = list(deserialized_embedding)
        id = str(uuid.uuid5(NAMESPACE, segment_id))

        operation_info = client.upsert(
            collection_name="videos_and_segments",
            wait=True,
            points=[
                PointStruct(id=id, vector=vector, payload=payload)
            ],
        )

        logger.info(f"QDrant Operation Info Status: {operation_info.status.value}")

        try:
            database.write_to_table(pd.DataFrame(
                [{
                    'segment_id': segment_id,
                    'video_id': video_id,
                    'uploaded_at': datetime.now()
                }]
            ), transcripts_segment_vectors_stored)
        except Exception as e:
            logger.info(f"Failed to upload row to stored tracker, {e}")

        count += 1


@flow
def store_shorts_to_vector_db(number_of_videos_to_upload: int = 50):
    database = ProductionDatabase()
    client = QdrantClient(url=os.getenv("QDRANT_LOCATION"))
    logger = get_run_logger()

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Not Found")

    if not database.table_exists(embeddings_transcript):
        raise Exception("Embeddings Transcripts Does Not Exist")

    if not database.table_exists(videos_embedded):
        raise Exception("Videos Embedding Tracker Does Not Exist")

    if not database.table_exists(videos_vectors_stored):
        raise Exception("Videos Vectors Stored Tracker Does Not Exist")

    if not client.collection_exists("videos_and_segments"):
        raise Exception("(QDRANT) Collection videos_and_segments Does Not Exist")

    # Load in tables to memory
    videos_cleaned_df = database.read_table(videos_cleaned)
    videos_embedded_df = database.read_table(videos_embedded)
    videos_vectors_stored_df = database.read_table(videos_vectors_stored)

    # Determine eligible videos
    videos_already_stored = set(videos_vectors_stored_df['video_id'])
    eligible_videos_df = videos_embedded_df[~videos_embedded_df['video_id'].isin(videos_already_stored)]

    count = 0

    for index, video_embedding in eligible_videos_df.iterrows():
        if count >= number_of_videos_to_upload:
            logger.info(f"Finished uploading {number_of_videos_to_upload} segments to vector db")

        video_id = video_embedding['video_id']

        video_rows = database.query_table_by_column(embeddings_transcript, 'video_id', video_id)

        if len(video_rows) < 1:
            logger.info(f"Unable to find entry for {video_id} in embeddings_transcript table.")
            continue

        # Get the embedding information
        embedding_entry = video_rows.iloc[0]
        videos_cleaned_info = videos_cleaned_df[videos_cleaned_df['video_id'] == video_id]

        if len(videos_cleaned_info) < 1:
            logger.info(f"Unable to find entry for {video_id} in videos_cleaned table.")
            continue

        logger.info(f"Attempting to upload Video ({video_id}) to vector db.")

        videos_cleaned_entry = videos_cleaned_info.iloc[0]

        # Extract original embedding
        serialized_embedding = embedding_entry['embedding']
        deserialized_embedding = pickle.loads(serialized_embedding)  # ~ np.array

        payload = {
            'video_id': video_id,
            'is_segment': False,
            'segment_id': "",
            'channel_id': videos_cleaned_entry['channel_id']
        }

        vector = list(deserialized_embedding)
        id = str(uuid.uuid5(NAMESPACE, video_id))

        operation_info = client.upsert(
            collection_name="videos_and_segments",
            wait=True,
            points=[
                PointStruct(id=id, vector=vector, payload=payload)
            ],
        )

        logger.info(f"QDrant Operation Info Status: {operation_info.status.value}")

        try:
            database.append_rows(pd.DataFrame(
                [{
                    'video_id': video_id,
                    'uploaded_at': datetime.now()
                }]
            ), videos_vectors_stored)
        except Exception as e:
            logger.info(f"Failed to upload row to stored tracker, {e}")

        count += 1


if __name__ == "__main__":
    store_segments_to_vector_db(1)



    '''
    # Storing Segments
    store_segments_to_vector_db.serve(
        name="Store Segments to Vector DB [VectorDB]",
        tags=["Upload", "Embeddings"],
    )

    # Storing Segments
    store_shorts_to_vector_db.serve(
        name="Store Shorts to Vector DB [VectorDB]",
        tags=["Upload", "Embeddings"],
    )
    '''
import pandas as pd

from database.production_database import ProductionDatabase
from table_names import videos_cleaned, channels_historical_stats, videos_evaluated
from datetime import datetime


def predict_video_score():
    database = ProductionDatabase()

    if not database.table_exists(videos_cleaned):
        raise Exception("Table Videos Cleaned Doesn't Exist")

    if not database.table_exists(channels_historical_stats):
        raise Exception("Table Channels Historical Data Doesn't Exist")

    if not database.table_exists(videos_evaluated):
        raise Exception("Table Channels Historical Data Doesn't Exist")

    videos_cleaned_df = database.read_table(videos_cleaned)
    channels_historical_stats_df = database.read_table(channels_historical_stats)
    videos_evaluated_df = database.read_table(videos_evaluated)

    # Ensure not to duplicate this
    videos_evaluated_already = set(videos_evaluated_df['video_id'])
    eligible_videos_df = videos_cleaned_df[~videos_cleaned_df['video_id'].isin(videos_evaluated_already)]

    information_collected = []

    for index, row in eligible_videos_df.iterrows():
        channel_id = row['channel_id']
        video_id = row['video_id']
        print(f"Getting video data for video {video_id}")
        publish_time = datetime.strptime(row['publish_time'], '%Y-%m-%d %H:%M:%S')
        view_count = row['view_count']
        like_count = row['like_count']
        favorite_count = row['favorite_count']
        comment_count = row['comment_count']

        # Extracting relevant information?
        channel_specific_history = channels_historical_stats_df[
            channels_historical_stats_df['channel_id'] == channel_id]
        subscriber_count_on_video = channel_specific_history[
            channel_specific_history['date'].dt.date == publish_time.date()]
        count = None
        if (len(subscriber_count_on_video) > 0):
            count = subscriber_count_on_video.iloc[0]['subs']

        # Also get the difference in days since last upload
        days_passed = (datetime.now().date() - publish_time.date()).days

        data = {
            'channel_id': channel_id,
            'video_id': video_id,
            'publish_time': row['publish_time'],
            'view_count': view_count,
            'like_count': like_count,
            'favorite_count': favorite_count,
            'comment_count': comment_count,
            'subscriber_count_on_video_release': subscriber_count_on_video,
            'days_passed': days_passed
        }

        information_collected.append(data)

    output_df = pd.DataFrame(information_collected)
    output_df.to_csv('output.csv')





if __name__ == "__main__":
    predict_video_score()
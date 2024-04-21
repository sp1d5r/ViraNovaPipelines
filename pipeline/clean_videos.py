from database.production_database import ProductionDatabase
from table_names import videos_raw, videos_cleaned
from datetime import datetime
from prefect import flow
import re

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return s2.replace(' ', '_')


"""
Clean Videos
"""

@flow
def clean_raw_videos():
    """
    Transformations Included:
    - Casts the correct types (string -> integer, string -> date time, string -> string)
    - Adds Download Link
    - Unsure if needed (Removes 0 views and stuff)
    """

    def cast_to_int(x):
        return int(x)

    def parse_duration(duration_string):
        # Match ISO 8601 duration format
        pattern = re.compile(
            'P'  # starts with 'P'
            '(?:(\d+)D)?'  # days
            'T'  # separator 'T'
            '(?:(\d+)H)?'  # hours
            '(?:(\d+)M)?'  # minutes
            '(?:(\d+)S)?'  # seconds
        )
        matches = pattern.match(duration_string)
        if not matches:
            return 0
        days, hours, minutes, seconds = matches.groups(default='0')
        # Convert all to seconds and return as milliseconds
        return (int(days) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)) * 1000

    def parse_published_time(x):
        return datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

    def get_youtube_url(video_id):
        base_url = "https://www.youtube.com/watch?v="
        return base_url + video_id

    database = ProductionDatabase()

    if not database.table_exists(videos_cleaned):
        raise Exception("Videos Cleaned Table Does Not Exist.")

    if not database.table_exists(videos_raw):
        raise Exception("Videos Raw Table Does Not Exist.")

    raw_videos_df = database.read_table(videos_raw)

    raw_videos_df['viewCount'] = raw_videos_df['viewCount'].apply(cast_to_int)
    raw_videos_df['likeCount'] = raw_videos_df['likeCount'].apply(cast_to_int)
    raw_videos_df['favoriteCount'] = raw_videos_df['favoriteCount'].apply(cast_to_int)
    raw_videos_df['commentCount'] = raw_videos_df['commentCount'].apply(cast_to_int)
    raw_videos_df['duration'] = raw_videos_df['duration'].apply(parse_duration)
    raw_videos_df['publishTime'] = raw_videos_df['publishTime'].apply(parse_published_time)
    raw_videos_df['download_link'] = raw_videos_df['videoId'].apply(get_youtube_url)

    raw_videos_df = raw_videos_df.drop_duplicates()

    raw_videos_df.columns = [to_snake_case(col) for col in raw_videos_df.columns]

    database.write_to_table(raw_videos_df, videos_cleaned)


if __name__ == "__main__":
    clean_raw_videos.serve(
        name="Clean Videos (Videos)",
        tags=["Data Cleaning", "Videos"],
    )

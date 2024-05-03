from database.production_database import ProductionDatabase
from services.social_blade import SocialBladeService
from table_names import channels_downloaded, channels_historical_stats


def get_historical_channel_data():
    database = ProductionDatabase()
    social_blade = SocialBladeService()

    if not database.table_exists(channels_downloaded):
        raise Exception("Table Channels Downloaded Doesn't Exist")

    if not database.table_exists(channels_historical_stats):
        raise Exception("Table Channels Historical Stats Doesn't Exist")

    channels_downloaded_df = database.read_table(channels_downloaded)
    channels_historical_stats_df = database.read_table(channels_historical_stats)

    collected_channel_info = set(channels_historical_stats_df['channel_id'])
    eligible_channels_df = channels_downloaded_df[~channels_downloaded_df['channel_id'].isin(collected_channel_info)]

    for index, row in eligible_channels_df.iterrows():
        channel_id = row['channel_id']
        print(f'Getting Historical Data for Channel: {channel_id}')

        social_blade_historical_data = social_blade.get_youtube_channel_stats(channel_id=channel_id, history='archive')

        if social_blade_historical_data is not None:
            database.append_rows(social_blade_historical_data, channels_historical_stats)
        else:
            print(f"Getting historical data for social blade")


if __name__ == '__main__':
    get_historical_channel_data()
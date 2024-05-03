import requests
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class SocialBladeService():
    def __init__(self):
        self.client_id = os.getenv('SOCIAL_BLADE_CLIENT_ID')
        self.token = os.getenv('SOCIAL_BLADE_TOKEN')

    def get_youtube_channel_stats(self, channel_id: str, history='default') -> pd.DataFrame:
        url = 'https://matrix.sbapis.com/b/youtube/statistics'
        headers = {
            "query": channel_id,
            "page": "0",
            "clientid": self.client_id,
            "token": self.token,
            "history": history
        }

        response = requests.get(url, headers=headers)

        print(response)
        if response.status_code == 200:
            response_json = response.json()

            if 'data' in response_json and 'daily' in response_json['data']:
                daily_subscriber_data = response_json['data']['daily']
                response_df = pd.DataFrame(daily_subscriber_data)
                response_df['channel_id'] = channel_id
                response_df['stat_id'] = response_df['date'].apply(lambda x: channel_id + '_' + x)

                # Type conversion.
                response_df['subs'] = pd.to_numeric(response_df['subs'])
                response_df['views'] = pd.to_numeric(response_df['views'])
                response_df['date'] = pd.to_datetime(response_df['date'])

                return response_df

        return None

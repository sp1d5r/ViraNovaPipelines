import time
import requests


class YoutubeDataCollector:
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self.BASE_URL = 'https://www.googleapis.com/youtube/v3/'

    def fetch_all_videos_from_channel(self, channel_id, max_total_results=200, sleep_in_s=20):
        video_ids = set()  # Use a set to store unique video IDs
        video_items = []
        next_page_token = None
        try:
            while len(video_items) < max_total_results:
                search_url = f"{self.BASE_URL}search"
                params = {
                    'key': self.API_KEY,
                    'channelId': channel_id,
                    'part': 'snippet',
                    'order': 'date',
                    'maxResults': 50
                }
                if next_page_token:
                    params['pageToken'] = next_page_token

                response = requests.get(search_url, params=params)

                if response.status_code != 200:
                    print(f"Failed to fetch data: {response.json()}")
                    break

                # Loop through the items and add unique video IDs to the set
                for item in response.json()['items']:
                    video_info = self._extract_relevant_info(item)
                    video_id = video_info.get('videoId')

                    if video_id not in video_ids:
                        video_ids.add(video_id)
                        video_items.append(video_info)

                next_page_token = response.json().get('nextPageToken')
                print(f"Next Page Token: {next_page_token}. Sleeping for: {sleep_in_s}")
                if not next_page_token:
                    break

                time.sleep(sleep_in_s)

            clean_video_items = self.append_video_details(list(video_items)[:max_total_results])

            return clean_video_items
        except Exception as e:
            print(f"ERROR TRYING TO GET THE VIDEOS FOR CHANNEL: {channel_id}")
            print(e)
            clean_video_items = self.append_video_details(list(video_items)[:max_total_results])

            return clean_video_items

    def fetch_all_video_ids_for_niche(self, niche, max_total_results=200):
        try:
            video_items = []
            next_page_token = None

            while len(video_items) < max_total_results:
                search_url = f"{self.BASE_URL}search"
                params = {
                    'key': self.API_KEY,
                    'q': niche,
                    'part': 'snippet',
                    'maxResults': 50,
                    'type': 'video',
                    'relevanceLanguage': 'en',
                    'videoLicense': 'creativeCommon',
                    'pageToken': next_page_token
                }
                response = requests.get(search_url, params=params)

                print(response)

                video_items += [self._extract_relevant_info(item) for item in response.json()['items']]

                next_page_token = response.json().get('nextPageToken')
                if not next_page_token:
                    break

            clean_video_items = self.append_video_details(video_items)

            return clean_video_items[:max_total_results]
        except Exception as e:
            print(f"ERROR TRYING TO GET THE VIDEOS FOR {niche}")
            print(e)
            return []

    def _extract_relevant_info(self, video_item):
        # Extract relevant data
        video_id = video_item['id']['videoId']
        snippet = video_item['snippet']
        channel_id = snippet['channelId']
        title = snippet['title']
        description = snippet['description']
        channel_title = snippet['channelTitle']
        publish_time = snippet['publishTime']

        # Extract width and height if available
        width = None
        height = None
        if 'thumbnails' in snippet and 'default' in snippet['thumbnails']:
            width = snippet['thumbnails']['default'].get('width')
            height = snippet['thumbnails']['default'].get('height')

        # Calculate aspect ratio
        aspect_ratio = None
        if width and height:
            aspect_ratio = width / height

        # Construct and return a cleaner dictionary with aspect ratio
        return {
            'videoId': video_id,
            'channel_id': channel_id,
            'title': title,
            'description': description,
            'channelTitle': channel_title,
            'publishTime': publish_time,
            'width': width,
            'height': height,
            'aspect_ratio': aspect_ratio  # Add the aspect ratio
        }

    def fetch_video_statistics(self, video_ids):
        video_url = f"{self.BASE_URL}videos"
        all_video_details = []

        # Split the video IDs into batches of 50
        for i in range(0, len(video_ids), 50):
            batch_video_ids = video_ids[i:i + 50]
            params = {
                'key': self.API_KEY,
                'id': ','.join(batch_video_ids),
                'part': 'contentDetails,statistics'
            }
            response = requests.get(video_url, params=params)
            all_video_details.extend(response.json()['items'])

        return all_video_details

    def append_video_details(self, video_items):
        # Extract video IDs from the video items
        video_ids = [item['videoId'] for item in video_items]

        # Fetch additional details for the video IDs
        video_details_list = self.fetch_video_statistics(video_ids)

        # Create a mapping from video ID to its additional details for easy lookup
        video_details_mapping = {details['id']: details for details in video_details_list}

        # Append the additional details to the video items
        for item in video_items:
            video_id = item['videoId']
            if video_id in video_details_mapping:
                details = video_details_mapping[video_id]
                item['viewCount'] = details['statistics'].get('viewCount', '0')
                item['likeCount'] = details['statistics'].get('likeCount', '0')
                item['favoriteCount'] = details['statistics'].get('favoriteCount', '0')
                item['commentCount'] = details['statistics'].get('commentCount', '0')
                item['duration'] = details['contentDetails'].get('duration', '0')
                item['projection'] = details['contentDetails'].get('projection', 'rectangular')

        return video_items

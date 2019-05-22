import requests, sys, time, os, argparse
import pandas as pd
from datetime import datetime

# List of simple to collect features
snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']

# Used to identify columns, currently hardcoded order
header = ["video_id"] + snippet_features + ["trending", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]

def setup(api_path, id_path):
    with open(api_path, 'r') as file:
        api_key = file.readline()

    with open(id_path) as file:
        id_codes = [x.rstrip() for x in file]

    channel_ids = []
    
    for id_code in id_codes:
        df = pd.read_csv(f"trending/trending_{id_code}_{datetime.today().strftime('%Y-%m-%d')}_videos.csv")
        channel_id = list(set(df['channelId'].values))
        #print(f"datos : {channel_id[:3]} proviene de {id_code}")
        channel_ids.append(channel_id)

    return api_key, channel_ids, id_codes

def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def get_uploads_id(channel_id):
    #print(channel_id)
    # Get the entire uploads playlist ID 
    request_url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    request = request.json()
    items = request.get('items', [])
    
    try:
        contentDetails = items[0]['contentDetails']['relatedPlaylists']
        uploadId = contentDetails.get('uploads','')
        return uploadId
    except:
        return -1

def api_request(channel_id):
    # Get the complete list of uploaded videos
    uploads_id = get_uploads_id(channel_id)
    if uploads_id == -1:
        return []
    request_url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=5&playlistId={uploads_id}&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        # We can assume something is wrong with the video if it has no statistics, often this means it has been deleted
        # so we can just skip it
        if "statistics" not in video:
            continue

        # A full explanation of all of these features can be found on the GitHub page for this project
        video_id = prepare_feature(video['id'])

        # Snippet and statistics are sub-dicts of video, containing the most useful info
        snippet = video['snippet']
        statistics = video['statistics']

        # This list contains all of the features in snippet that are 1 deep and require no special processing
        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]

        # The following are special case features which require unique processing, or are not within the snippet dict
        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending = 0
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        # This may be unclear, essentially the way the API works is that if a video has comments or ratings disabled
        # then it has no feature for it, thus if they don't exist in the statistics dict we know they are disabled
        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0

        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        # Compiles all of the various bits of info into one consistently formatted line
        line = [video_id] + features + [prepare_feature(x) for x in [trending, tags, view_count, likes, dislikes,
                                                                       comment_count, thumbnail_link, comments_disabled,
                                                                       ratings_disabled, description]]
        lines.append(",".join(line))
    return lines

def get_pages(channel_ids):
    channel_data = []

    # Because the API uses page tokens (which are literally just the same function of numbers everywhere) it is much
    # more inconvenient to iterate over pages, but that is what is done here.
    # A page of data i.e. a list of videos and all needed data
    for channel_id in channel_ids:
        uploaded_videos = api_request(channel_id)
        if(uploaded_videos == []): break

        items = uploaded_videos.get('items',[])
        
        for item in items:
            snippet = item['snippet']
            videoId = snippet.get('resourceId','').get('videoId','')
            
            request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet&id={videoId}&key={api_key}"
            request = requests.get(request_url)
            if request.status_code == 429:
                print("Temp-Banned due to excess requests, please wait and continue later")
                sys.exit()
            request = request.json()
            request_item = request.get('items','')
            
            channel_data += get_videos(request_item)
    
    return channel_data

def write_to_file(channel_id, channel_data, country_code):
    
    print(f"Writing {country_code} data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(f"{output_dir}/no_trending_{country_code}_{datetime.today().strftime('%Y-%m-%d')}_videos.csv", "w+", encoding='utf-8') as file:
        for row in channel_data:
            file.write(f"{row}\n")

def get_data():
    count = 0
    for channel_id in channel_ids:
        channel_data = [",".join(header)] + get_pages(channel_id)
        write_to_file(channel_id, channel_data, country_codes[count])
        count += 1

        
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--key_path', help='Path to the file containing the api key, by default will use api_key.txt in the same directory', default='api_key.txt')
    parser.add_argument('--country_code_path', help='Path to the file containing the list of country codes to scrape, by default will use country_codes.txt in the same directory', default='country_codes.txt')
    parser.add_argument('--output_dir', help='Path to save the outputted files in', default='output/')

    args = parser.parse_args()

    output_dir = args.output_dir
    api_key, channel_ids, country_codes = setup(args.key_path,args.country_code_path)

    get_data()
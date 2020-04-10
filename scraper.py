import requests, sys, time, os, argparse, json, csv, pickle, re

# List of simple to collect features
snippet_features = ["title", "publishedAt", "channelId", "channelTitle", "categoryId"]

# Used to identify columns, currently hardcoded order
header = ["rank", "video_id"] +  ["title", "publishedAt", "channelId", "channelTitle", "categoryId"] + ["category", "trending_date", "tags", "view_count", "likes", "dislikes", "comment_count", "duration"]
                                            
things_that_i_dont_want_to_include = ["thumbnail_link", "comments_disabled","ratings_disabled", "description"]

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']

cats = {1: 'Film & Animation', 2: 'Autos & Vehicles', 10: 'Music', 15: 'Pets & Animals', 17: 'Sports', 18: 'Short Movies', 19: 'Travel & Events', 20: 'Gaming', 21: 'Videoblogging', 22: 'People & Blogs', 23: 'Comedy', 24: 'Entertainment', 25: 'News & Politics', 26: 'Howto & Style', 27: 'Education', 28: 'Science & Technology', 29: 'Nonprofits & Activism', 30: 'Movies', 31: 'Anime/Animation', 32: 'Action/Adventure', 33: 'Classics', 34: 'Comedy', 35: 'Documentary', 36: 'Drama', 37: 'Family', 38: 'Foreign', 39: 'Horror', 40: 'Sci-Fi/Fantasy', 41: 'Thriller', 42: 'Shorts', 43: 'Shows', 44: 'Trailers'}

def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(pageToken='', by_cat=False, trending=True, catId='', items=[], n=0):
#     Calls the API and returns a list of items. It has to iterate through recursively
#     a few times because the full response is split up into multiple pages
    
    url_base  = 'https://www.googleapis.com/youtube/v3/videos?'
    features  =f'part=snippet%2CcontentDetails%2Cstatistics&{pageToken}'
    category  =f'videoCategoryId={catId}&' if by_cat else ''
    chart     = 'chart=mostPopular&' if trending else ''
    other     =f'{chart}regionCode=US&{category}key='
    url       =  url_base + features + other + api_key
    response  =  requests.get(url)
    
    if response.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    response = response.json()
    
    itms_lst  =  response['items']
    for item in itms_lst:
        n+=1
        item['rank'] = n
        items.append(item)
        
#     The response doesn't return all the results. Use the token to get the next page
    token    =  response.get("nextPageToken", None)
    if token != None:
        pageToken = f'pageToken={token}&'
        api_request(pageToken=pageToken, items = items, n=n)
    timestamp = time.strftime('%y.%m.%d.%H:%M')
    with open(f'items_{timestamp}.pickle', 'wb') as pickly:
        pickle.dump(items, pickly)    
    return items


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def calc_seconds(duration):
#     Takes the YouTube duration field and calculated seconds
    hms = 'HMS'
    nums = [0,0,0]
    pattern = '[0-9]+[HMS]'
    result = re.findall(pattern, duration)
    #result for 10 hours, 1 minute, 26 seconds = ['10H', '1M', '26S']
    for val in result:
        nums[hms.index(val[-1])] = int(val[:-1])    
    seconds = nums[0] * 60 * 60 + nums[1]*60 + nums[2]
    return seconds

def parse_videos(items):
    lines = []
    for video in items:
        # We can assume something's wrong with a video with no stats, likely deleted
        if "statistics" not in video:
            continue
        # A full explanation of all of these features can be found on the GitHub page for this project
        rank = video['rank']       #I added this rank during processing
        video['video_id'] = video['id']

        # Snippet and statistics are sub-dicts of video, containing the most useful info
        snippet = video['snippet']
        statistics = video['statistics']
        contentDetails = video['contentDetails']

        # This list contains all of the features in snippet that are 1 deep and require no special processing
        for feature in snippet_features[:-1]:
            video[feature] = snippet.get(feature, "") 
        video['categoryId'] = int(snippet.get('categoryId'))
#         Add the text of the category
        video['category'] = cats[video['categoryId']]

        # The following are special case features which require unique processing, or are not within the snippet dict
#         description = snippet.get("description", "")
        video['trending_date'] = time.strftime("%y.%m.%d")
        video['tags'] = get_tags(video['snippet'].get("tags", ["[none]"]))
        video['view_count'] = int(video['statistics'].get("viewCount", 0))
        video['duration'] = calc_seconds(video['contentDetails'].get("duration", 0))
        video['likes'] = int(video['statistics'].get('likeCount', 0))
        video['dislikes'] = int(video['statistics'].get('dislikeCount', 0))
        video['comment_count'] = int(video['statistics'].get('commentCount', 0))
        new_video = {x:video[x] for x in header}
        lines.append(new_video)
    
    with open(f'videos_{timestamp}.pickle', 'wb') as pickly:
        pickle.dump(lines, pickly)   
    return lines


def write_to_file(lines):
    print(f"Writing video data to file {time.strftime('%y.%m.%d.%H:%M')}...")

    with open(f"{output_dir}/{time.strftime('%y.%m.%d.%H:%M')}_US_videos.csv", "w+", encoding='utf-8') as file:
        for row in lines:
            file.write(f"{row}\n")
    with open(f"{output_dir}/videos.csv", "a+", encoding='utf-8') as file:
        for row in lines:
            csv.writer(file).writerow(row)
            print('Done with that one')

if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--key_path', help='Path to the file containing the api key, by default will use api_key.txt in the same directory', default='api_key.txt')
#     parser.add_argument('--country_code_path', help='Path to the file containing the list of country codes to scrape, by default will use country_codes.txt in the same directory', default='country_codes.txt')
#     parser.add_argument('--output_dir', help='Path to save the outputted files in', default='data/')
#     args = parser.parse_args()
    while True:
        output_dir = 'data/'
        with open('api_key.txt', 'r') as file:
            api_key = file.read().rstrip()
        for cat in cats:
            lines = parse_videos(api_request(by_cat=True, trending=True, catId=cat))
            write_to_file(lines)
            os.sleep(5)
        lines = parse_videos(api_request(by_cat=False))
        write_to_file(lines)
            
        os.sleep(1800)
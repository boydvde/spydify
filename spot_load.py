import os, ssl, json, time
import urllib.request, urllib.error, urllib.parse
from dotenv import load_dotenv

from spot_access import get_token, login

# Load the environment variables and define file paths
load_dotenv()
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
ACCESS_TOKEN_PATH = "temp/access_token"
REFRESH_TOKEN_PATH = "temp/refresh_token"

# Create an SSL context to ignore certificate verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_info(item_type, item_id, retries=3):
    """
    Retrieve information from Spotify for a specific item type and ID.

    Args:
        item_type (str): The type of item to retrieve. Must be one of 'tracks', 'albums', or 'artists'.
        item_id (str): The unique identifier for the item.
        token (str): The access token for Spotify API authentication.

    Returns:
        dict: The information retrieved from Spotify for the specified item.

    Raises:
        ValueError: If the item_type is not one of 'track', 'album', 'artist' or 'playlist'.
    """
    valid_types = ['track', 'album', 'artist', 'playslist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}s/{item_id}', method="GET")
    req.add_header('Authorization', f'Bearer {get_token()}')
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            return json.loads(content)
    # Recursively retry the request if rate limited (max retries = 3), else print the error
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return get_info(item_type, item_id, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e.msg}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

def get_batch_info(item_type, item_ids, retries=3):
    valid_types = ['tracks', 'albums', 'artists']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")
    
    if len(item_ids) == 0: return None
    elif len(item_ids) > 50: raise ValueError("Maximum number of items is 50")
    
    ids = ','.join(item_ids)
    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}?ids={ids}', method="GET")
    req.add_header('Authorization', f'Bearer {get_token()}')
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            return json.loads(content)
    # Recursively retry the request if rate limited (max retries = 3), else print the error
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return get_batch_info(item_type, item_id, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e.msg}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None
    
def get_user_saved(token):
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        req = urllib.request.Request(f'https://api.spotify.com/v1/me/tracks?limit={limit}&offset={offset}', method="GET")
        req.add_header('Authorization', f'Bearer {token}')
        try:
            with urllib.request.urlopen(req) as r:
                content = r.read().decode()
                js = json.loads(content)
                total = js['total']
                items.extend(js['items'])
                offset += limit
        except urllib.error.HTTPError as e:
            print(f"HTTPError: {e.code} {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return items

def pretty_print(item_type, data):
    i = 1
    if item_type == 'track':
        print(f"{data['name']} by {data['artists'][0]['name']}")
    elif item_type == 'album':
        print(f"\nAlbum: {data['name']} by {data['artists'][0]['name']}")
        for track in data['tracks']['items']:
            print(f'{i}.', track['name'])
    elif item_type == 'artist':
        print(f"\nArtist: {data['name']}")
    elif item_type == 'playlist':
        print(f"\nPlaylist: {data['name']} by {data['owner']['display_name']}")
        for track in data['tracks']['items']:
            print(f'{i}.', track['track']['name'], "by", track['track']['artists'][0]['name'])

if __name__ == "__main__":
    # Check if logged in, else login
    if not os.path.exists(REFRESH_TOKEN_PATH) or get_token() is None: login()

    # Main menu
    try:
        choice = input(
'''
What would you like to do?
    1. Get user saved tracks
    2. Get info (track/album/artist/playlist)
    3. Get batch info (tracks/albums/artists)

Enter you choice: ''')
    except KeyboardInterrupt:
        print("\nExiting...")
        exit(0)

    # Get user saved tracks
    if choice == '1':
        for track in get_user_saved(get_token()):
            pretty_print('track', track['track'])
    
    # Get info (track/album/artist/playlist)
    elif choice == '2':
        item_type = input("Enter the item type (tracks, albums, artists or playlists): ")
        item_id = input("Enter the Spotify ID: ")
        info = get_info(item_type, item_id)
        if info is not None: pretty_print(item_type, info)

    # Get batch info (tracks/albums/artists)
    elif choice == '3':
        inp = '''6cP6IST6zj0sPIDOjmA1JZ
5mbEDRNFzwWFGSW3f7guHB
0aMonkh8OKgqx1K0viRHRT
7iWWLbTuSYdncjX1tT22JJ
0JCG4FU6reipiFnJ0sGloH
1wCWu0olvm0XqvaY0CNna9
50nexV89Lzk96Nw2WYCpXj
41Pezz8jOFGYtax20GRwAJ
0cAhbpsVMIeAoNsmLWQvZ9
78caY2380YY6y4EYW5xx1m
1N5nFD10jc1DhHh05ClXmD
6vvdKKpfb645nvLkO2C1tH
13JMGEaIAXzOndW8ETk7wC
6PrPWf02VxGUd2jJLs9z1M
3LDzO5Cz3hxdpfLSa6VsNr
50aNLhnlmcuJQ2iF7Bpd6q
1xZ9D4HLdm1PdjCwxik73W
7n24EOW7ElKwtz5wXkzynQ
6v2N2miIqFLuOLbyFNeAns
55EPegD9KficI9lrBqqnwG
6XKLjJ3MaAH32e5xILIFaL'''
        ids = [line.strip() for line in inp.split('\n')]
        info = get_batch_info('tracks', ids)
        if info is not None:
            for track in info['tracks']:
                pretty_print('track', track)

    # Exit
    else:
        print("Invalid choice. Exiting...")
        exit(0)

'''Spider logic 
1. Get user saved tracks
2. Get all item types + spotify ID
3. Add to queue
4. Get info for each item in the queue
5. Add to queue if not visited
6. Repeat until queue is empty'''
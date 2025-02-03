import os, ssl, json, time, datetime, sqlite3
import urllib.request, urllib.error, urllib.parse
from collections import deque
from dotenv import load_dotenv
from spot_access import get_token, login

# Load the environment variables and define file paths
load_dotenv()
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
ACCESS_TOKEN_PATH = "temp/access_token"
REFRESH_TOKEN_PATH = "temp/refresh_token"
DEBUG = True

# Create an SSL context to ignore certificate verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Global deque to store the timestamps of the requests
request_timestamps = deque()

def add_request_count():
    """
    Add the current timestamp to the deque, remove timestamps older than 30 seconds.
    """
    current_time = time.time()
    request_timestamps.append(current_time)
    # Remove timestamps older than 30 seconds
    while request_timestamps and current_time - request_timestamps[0] > 30:
        request_timestamps.popleft()

def get_request_count():
    """
    Returns:
        int: The number of requests made in the last 30 seconds.
    """
    return len(request_timestamps)

def check_rate_limit():
    """
    Check if the rate limit has been reached and wait if necessary.
    """
    request_count = get_request_count()
    print(f'Requests in the last 30 seconds: {request_count}')
    if request_count >= 30:
        print("Rate limited. Waiting 30 seconds...")
        time.sleep(30)

def get_info(item_type, item_id, retries=3):
    """
    Retrieve information from Spotify for a specific item type and ID.

    Args:
        item_type (str): The type of item to retrieve. Must be one of 'tracks', 'albums', or 'artists'.
        item_id (str): The unique identifier for the item.

    Returns:
        dict: The information retrieved from Spotify for the specified item.

    Raises:
        ValueError: If the item_type is not one of 'track', 'album', 'artist' or 'playlist'.
    """
    valid_types = ['track', 'album', 'artist', 'playlist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}s/{item_id}', method="GET")
    req.add_header('Authorization', f'Bearer {get_token()}')
    check_rate_limit() # Check rate limit before making the request
    add_request_count() # Add the current timestamp to the deque
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            return json.loads(content)
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
    valid_types = ['track', 'album', 'artist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")
    
    if len(item_ids) == 0: return None
    elif item_type == 'track' and len(item_ids) > 50: raise ValueError("Max batch size of tracks is 50")
    elif item_type == 'artist' and len(item_ids) > 50: raise ValueError("Max batch size of artists is 50")
    elif item_type == 'album' and len(item_ids) > 20: raise ValueError("Max batch size of albums is 20")
    
    ids = ','.join(item_ids)
    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}s?ids={ids}', method="GET")
    req.add_header('Authorization', f'Bearer {get_token()}')
    check_rate_limit() # Check rate limit before making the request
    add_request_count() # Add the current timestamp to the deque
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            return json.loads(content)
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return get_batch_info(item_type, item_ids, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e.msg}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None
    
def get_user_saved():
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        req = urllib.request.Request(f'https://api.spotify.com/v1/me/tracks?limit={limit}&offset={offset}', method="GET")
        req.add_header('Authorization', f'Bearer {get_token()}')
        check_rate_limit() # Check rate limit before making the request
        add_request_count() # Add the current timestamp to the deque
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

def get_artist_albums(artist_id, retries=3):
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        req = urllib.request.Request(f'https://api.spotify.com/v1/artists/{artist_id}/albums?limit={limit}&offset={offset}', method="GET")
        req.add_header('Authorization', f'Bearer {get_token()}')
        check_rate_limit() # Check rate limit before making the request
        add_request_count() # Add the current timestamp to the deque
        try:
            with urllib.request.urlopen(req) as r:
                content = r.read().decode()
                js = json.loads(content)
                total = js['total']
                items.extend(js['items'])
                offset += limit
        except urllib.error.HTTPError as e:
            if e.code == 429 and retries > 0:
                retry_after = int(e.headers.get('Retry-After', 1))
                print(f"Rate limited. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                return get_artist_albums(artist_id, retries - 1)
            print(f"HTTPError: {e.code} {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return items

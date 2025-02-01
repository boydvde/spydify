import os, ssl, json, time, sqlite3
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
            return get_batch_info(item_type, item_ids, retries - 1)
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

def get_related_artists(artist_id):
    req = urllib.request.Request(f'https://api.spotify.com/v1/artists/{artist_id}/related-artists', method="GET")
    req.add_header('Authorization', f'Bearer {get_token()}')
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            js = json.loads(content)
            return js['artists']
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code} {e.reason}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

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

def base62_decode(base62_str):
    base62_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    base = len(base62_chars)
    decoded_value = 0
    for char in base62_str:
        decoded_value = decoded_value * base + base62_chars.index(char)
    return decoded_value

def base62_encode(value):
    base62_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    base = len(base62_chars)
    encoded_str = ""
    while value > 0:
        encoded_str = base62_chars[value % base] + encoded_str
        value //= base
    return encoded_str

def __generate_debug_json():
    '''
    Generate JSON files for debugging purposes
    '''
    # Save track info for debugging
    track = get_info('track', '3AwmE4xsRfgjtIPLMyvL9i')
    json.dump(track, open("debug/track.json", "w"), indent=2) 

    # Save album info for debugging
    album = get_info('album', '29sKvBpV3odDmSp5Cc3P1V')
    json.dump(album, open("debug/album.json", "w"), indent=2)

    # Save artist info for debugging
    artist = get_info('artist', '7sJ3ngSMvvXGdVLnODPqXa')
    json.dump(artist, open("debug/artist.json", "w"), indent=2)

if __name__ == "__main__":
    # Check if logged in, else login
    if not os.path.exists(REFRESH_TOKEN_PATH) or get_token() is None: login()

    # Connect to the SQLite database
    os.makedirs("db", exist_ok=True)
    conn = sqlite3.connect("db/spotify.sqlite")
    cursor = conn.cursor()

    # Track table: id, name, [artist_id], album_id, duration, popularity, explicit, track_number
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Track (
            id TEXT PRIMARY KEY,
            name TEXT,
            album_id TEXT,
            duration INTEGER,
            popularity INTEGER,
            explicit INTEGER,
            track_number INTEGER
        )
    ''')

    # Album table: id, name, [artist_id], release_date, total_tracks, label, album_type, popularity
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Album (
            id TEXT PRIMARY KEY,
            name TEXT,
            artist_id TEXT,
            release_date TEXT,
            total_tracks INTEGER,
            label TEXT,
            album_type TEXT,
            popularity INTEGER
        )
    ''')
    
    # Artist table: id, name, genres, popularity, followers
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Artist (
            id TEXT PRIMARY KEY,
            name TEXT,
            genres TEXT,
            popularity INTEGER,
            followers INTEGER
        )
    ''')

    # Create a connector table for the many-to-many relationship between tracks and artists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TrackArtist (
            track_id TEXT,
            artist_id TEXT,
            PRIMARY KEY (track_id, artist_id),
            FOREIGN KEY (track_id) REFERENCES Track(id),
            FOREIGN KEY (artist_id) REFERENCES Artist(id)
        )
    ''')

    # Create a connector table for the many-to-many relationship between albums and artists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AlbumArtist (
            album_id TEXT,
            artist_id TEXT,
            PRIMARY KEY (album_id, artist_id),
            FOREIGN KEY (album_id) REFERENCES Album(id),
            FOREIGN KEY (artist_id) REFERENCES Artist(id)
        )
    ''')

    conn.commit()

    # TODO: Spider logic 
    # 1. Get user saved tracks info
    # 2. Add track info to database
    # 3. Add artist and album id's to queue
    # 4. Get track id's from albums (batch request) and add to queue
    # 5. Get track id's from artists (batch request) and add to queue
    # 6. Get related artists and add to queue
    # 7. Repeat until queue is empty
    
    saved_tracks = get_user_saved(get_token())
    for track in saved_tracks:
        track_id = track['track']['id']
        track_name = track['track']['name']     
        artist_ids = [artist['id'] for artist in track['track']['artists']] 
        album_id = track['track']['album']['id']
        duration = int(track['track']['duration_ms'])
        popularity = int(track['track']['popularity'])
        explicit = int(track['track']['explicit'])
        track_number = int(track['track']['track_number'])

        print(track_id, track_name, album_id, duration, popularity, explicit, track_number)
        # Insert track info into the database
        cursor.execute('''
            INSERT OR IGNORE INTO Track (id, name, album_id, duration, popularity, explicit, track_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (track_id, track_name, album_id, duration, popularity, explicit, track_number))
    
    conn.commit()

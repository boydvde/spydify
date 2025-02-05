import os, ssl, json, time, sqlite3
import urllib.request, urllib.error, urllib.parse
from collections import deque
from dotenv import load_dotenv
from spot_access import get_user_token, get_server_token, login

# Load the environment variables
load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
ACCESS_TOKEN_PATH = os.getenv('ACCESS_TOKEN_PATH')
REFRESH_TOKEN_PATH = os.getenv('REFRESH_TOKEN_PATH')
DEBUG = os.getenv('DEBUG', 'False').lower() in ('1', 'true', 'yes')

print("Debug mode:", DEBUG)

# Create an SSL context to ignore certificate verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Rate limiting
REQUEST_LOG_PATH = os.getenv('REQUEST_LOG_PATH')
MAX_REQUESTS_PER_30_SEC = 50 # Max requests per 30 seconds
MAX_REQUESTS_PER_HOUR = 4000 # Max requests per hour
MAX_REQUESTS_PER_DAY = 30000 # Max requests per day

# Global deque to store the timestamps of the requests
halfmin_timestamps = deque()
hourly_timestamps = deque()
daily_timestamps = deque()
response_times = deque(maxlen=10)
base_wait = 0.1 # Base wait time in seconds
total_requests = 0

def load_request_log():
    try:
        with open(REQUEST_LOG_PATH, 'r') as f:
            logs = json.load(f)
            global total_requests, halfmin_timestamps, hourly_timestamps, daily_timestamps
            total_requests = logs['total_requests']
            halfmin_timestamps = deque(logs['halfmin_timestamps'])
            hourly_timestamps = deque(logs['hourly_timestamps'])
            daily_timestamps = deque(logs['daily_timestamps'])
    except FileNotFoundError:
        print("Request log file not found. Starting fresh.")
    except json.JSONDecodeError:
        print("Error decoding request log file. Starting fresh.")

def save_request_log():
    logs = {
        'total_requests': total_requests,
        'halfmin_timestamps': list(halfmin_timestamps),
        'hourly_timestamps': list(hourly_timestamps),
        'daily_timestamps': list(daily_timestamps)
    }
    with open(REQUEST_LOG_PATH, 'w') as f:
        json.dump(logs, f)

def check_rate_limit():
    """
    Ensures requests stay within Spotify's API limits (30-sec, hourly, daily).
    If limits are exceeded, it waits before making the next request.
    """
    global total_requests, halfmin_timestamps, hourly_timestamps, daily_timestamps
    global response_times, base_wait

    current_time = time.time()

    if DEBUG:
        print(f"Total requests: {total_requests}")
        print(f"Requests in last 30 seconds: {len(halfmin_timestamps)}")
        print(f"Requests in last hour: {len(hourly_timestamps)}")
        print(f"Requests in last day: {len(daily_timestamps)}")
        print(f"Waiting {base_wait:.2f} seconds before next request...")

    time.sleep(base_wait) # Wait before making the request

    # 30-second rate limit
    if len(halfmin_timestamps) >= MAX_REQUESTS_PER_30_SEC:
        wait_time = 30 - (current_time - halfmin_timestamps[0])
        print(f"Rate limited: Waiting {wait_time:.2f} seconds to avoid 30-sec limit...")
        time.sleep(wait_time + 1)

    # Hourly rate limit
    if len(hourly_timestamps) >= MAX_REQUESTS_PER_HOUR:
        wait_time = 3600 - (current_time - hourly_timestamps[0])
        print(f"Hourly limit reached: Waiting {wait_time / 60:.2f} minutes...")
        time.sleep(wait_time + 1)

    # Daily rate limit
    if len(daily_timestamps) >= MAX_REQUESTS_PER_DAY:
        wait_time = 86400 - (current_time - daily_timestamps[0])
        print(f"Daily limit reached: Waiting {wait_time / 3600:.2f} hours...")
        time.sleep(wait_time + 1)

    # Calculate the average response time and adjust the wait time
    if len(response_times) > 5:
        avg_response_time = sum(response_times) / len(response_times)
        base_wait = max(0.1, avg_response_time * 1.5)
        if DEBUG:
            print(f"Adjusted base wait time: {base_wait}")

    # Add the current timestamp to the deque
    halfmin_timestamps.append(current_time)
    hourly_timestamps.append(current_time)
    daily_timestamps.append(current_time)
    total_requests += 1

    # Remove old timestamps from the deque
    while halfmin_timestamps and current_time - halfmin_timestamps[0] > 30:
        halfmin_timestamps.popleft()
    while hourly_timestamps and current_time - hourly_timestamps[0] > 3600:
        hourly_timestamps.popleft()
    while daily_timestamps and current_time - daily_timestamps[0] > 86400:
        daily_timestamps.popleft()

def get_info(item_type, item_id, retries=3):
    """
    Retrieve information from Spotify for a specific item type and ID.

    Args:
        item_type (str): The type of item to retrieve. Must be one of 'track', 'album', 'artist', or 'playlist'.
        item_id (str): The unique identifier for the item.
        retries (int, optional): The number of retry attempts in case of rate limiting. Defaults to 3.

    Returns:
        dict: The information retrieved from Spotify for the specified item if the request is successful.
        None: If the request fails.

    Raises:
        ValueError: If the item_type is not valid.
        urllib.error.HTTPError: If an HTTP error occurs during the request.
        json.JSONDecodeError: If there is an error decoding the JSON response.
        Exception: For any other unexpected errors.
    """
    valid_types = ['track', 'album', 'artist', 'playlist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}s/{item_id}', method="GET")
    req.add_header('Authorization', f'Bearer {get_server_token()}')
    check_rate_limit() # Check rate limit before making the request
    start_time = time.time()
    try:
        with urllib.request.urlopen(req) as r:
            response_times.append(time.time() - start_time)
            content = r.read().decode()
            return json.loads(content)
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            wait_time = retry_after * (2 ** (3 - retries))
            print(f"Rate limited. Retrying after {wait_time} seconds...")
            time.sleep(wait_time)
            return get_info(item_type, item_id, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e.msg}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

def get_batch_info(item_type, item_ids, retries=3):
    """
    Fetches batch information for a given item type and list of item IDs from the Spotify API.
    Args:
        item_type (str): The type of item to fetch. Must be one of 'track', 'album', or 'artist'.
        item_ids (list): A list of item IDs to fetch information for.
        retries (int, optional): The number of retry attempts in case of rate limiting. Defaults to 3.
    Returns:
        dict: A dictionary containing the batch information if the request is successful.
        None: If the request fails or if the item_ids list is empty.
    Raises:
        ValueError: If the item_type is not valid or if the batch size exceeds the allowed limit.
        urllib.error.HTTPError: If an HTTP error occurs during the request.
        json.JSONDecodeError: If there is an error decoding the JSON response.
        Exception: For any other unexpected errors.
    """

    valid_types = ['track', 'album', 'artist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")
    
    if len(item_ids) == 0: return None
    elif item_type == 'track' and len(item_ids) > 50: raise ValueError("Max batch size of tracks is 50")
    elif item_type == 'artist' and len(item_ids) > 50: raise ValueError("Max batch size of artists is 50")
    elif item_type == 'album' and len(item_ids) > 20: raise ValueError("Max batch size of albums is 20")
    
    ids = ','.join(item_ids)
    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}s?ids={ids}', method="GET")
    req.add_header('Authorization', f'Bearer {get_server_token()}')
    check_rate_limit() # Check rate limit before making the request
    start_time = time.time()
    try:
        with urllib.request.urlopen(req) as r:
            response_times.append(time.time() - start_time)
            content = r.read().decode()
            return json.loads(content)
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            wait_time = retry_after * (2 ** (3 - retries))
            print(f"Rate limited. Retrying after {wait_time} seconds...")
            time.sleep(wait_time)
            return get_batch_info(item_type, item_ids, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e.msg}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None
    
def get_user_saved(retries=3):
    """
    Retrieves the user's saved tracks from Spotify.
    This function makes a request to the Spotify API to fetch the user's saved tracks.
    It handles pagination by iterating through the results using the `limit` and `offset` parameters.
    If the request is rate-limited, it will retry up to the specified number of retries with exponential backoff.
    Args:
        retries (int): The number of times to retry the request in case of rate limiting. Default is 3.
    Returns:
        list: A list of saved track items retrieved from the Spotify API.
    Raises:
        urllib.error.HTTPError: If an HTTP error occurs that is not related to rate limiting.
        Exception: For any other unexpected errors.
    """
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        req = urllib.request.Request(f'https://api.spotify.com/v1/me/tracks?limit={limit}&offset={offset}', method="GET")
        req.add_header('Authorization', f'Bearer {get_user_token()}')
        check_rate_limit() # Check rate limit before making the request
        start_time = time.time()
        try:
            with urllib.request.urlopen(req) as r:
                response_times.append(time.time() - start_time)
                content = r.read().decode()
                js = json.loads(content)
                total = js['total']
                items.extend(js['items'])
                offset += limit
        except urllib.error.HTTPError as e:
            if e.code == 429 and retries > 0:
                retry_after = int(e.headers.get('Retry-After', 1))
                wait_time = retry_after * (2 ** (3 - retries))
                print(f"Rate limited. Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                return get_user_saved(retries - 1)
            print(f"HTTPError: {e.code} {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return items

def get_artist_albums(artist_id, retries=3):
    """
    Fetches all albums for a given artist from the Spotify API.
    Args:
        artist_id (str): The Spotify ID of the artist.
        retries (int, optional): The number of retries in case of rate limiting. Defaults to 3.
    Returns:
        list: A list of album items for the given artist.
    Raises:
        urllib.error.HTTPError: If an HTTP error occurs that is not related to rate limiting.
        Exception: For any other unexpected errors.
    """
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        url = f'https://api.spotify.com/v1/artists/{artist_id}/albums?limit={limit}&offset={offset}&include_groups=album,single'
        req = urllib.request.Request(url=url, method="GET")
        req.add_header('Authorization', f'Bearer {get_server_token()}')
        check_rate_limit() # Check rate limit before making the request
        start_time = time.time()
        try:
            with urllib.request.urlopen(req) as r:
                response_times.append(time.time() - start_time)
                content = r.read().decode()
                js = json.loads(content)
                total = js['total']
                items.extend(js['items'])
                offset += limit
        except urllib.error.HTTPError as e:
            if e.code == 429 and retries > 0:
                retry_after = int(e.headers.get('Retry-After', 1))
                wait_time = retry_after * (2 ** (3 - retries))
                print(f"Rate limited. Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                return get_artist_albums(artist_id, retries - 1)
            print(f"HTTPError: {e.code} {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return items

def create_tables(cursor):
    """
    Creates the necessary tables for the music database if they do not already exist.
    Tables created:
    - Track: Stores information about tracks, connected to Artist by TrackArtist and to Album by album_id.
    - Album: Stores information about albums, connected to Artist by AlbumArtist and to Track by album_id.
    - Artist: Stores information about artists, connected to Track by TrackArtist, to Album by AlbumArtist, and to Genre by ArtistGenre.
    - Genre: Stores information about genres, connected to Artist by ArtistGenre.
    - TrackArtist: Connector table for the many-to-many relationship between tracks and artists.
    - AlbumArtist: Connector table for the many-to-many relationship between albums and artists.
    - ArtistGenre: Connector table for the many-to-many relationship between artists and genres.
    Args:
        cursor (sqlite3.Cursor): The database cursor used to execute SQL commands.
    """
    # Track table: id, name, album_id, duration, popularity, explicit, track_number 
    #   connected to Artist by TrackArtist connector table
    #   connected to Album by album_id
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

    # Album table: id, name, release_date, total_tracks, label, album_type, popularity 
    #   connected to Artist by AlbumArtist connector table
    #   connected to Track by album_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Album (
            id TEXT PRIMARY KEY,
            name TEXT,
            release_date TEXT,
            total_tracks INTEGER,
            label TEXT,
            album_type TEXT,
            popularity INTEGER
        )
    ''')
    
    # Artist table: id, name, popularity, followers
    #   connected to Track by TrackArtist connector table
    #   connected to Album by AlbumArtist connector table
    #   connected to Genre by ArtistGenre connector table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Artist (
            id TEXT PRIMARY KEY,
            name TEXT,
            popularity INTEGER,
            followers INTEGER
        )
    ''')

    # Genre table: id, name
    #   connected to Artist by ArtistGenre connector table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Genre (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
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

    # Create a connector table for the many-to-many relationship between artists and genres
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ArtistGenre (
            artist_id TEXT,
            genre_id INTEGER,
            PRIMARY KEY (artist_id, genre_id),
            FOREIGN KEY (artist_id) REFERENCES Artist(id),
            FOREIGN KEY (genre_id) REFERENCES Genre(id)
        )
    ''')

def delete_tables(cursor):
    """
    Deletes the following tables from the music database:
    - Track
    - Album
    - Artist
    - Genre
    - TrackArtist
    - AlbumArtist
    - ArtistGenre

    Args:
        cursor (sqlite3.Cursor): The database cursor used to execute SQL commands.
    """
    
    cursor.executescript('''
        DROP TABLE IF EXISTS Track;
        DROP TABLE IF EXISTS Album;
        DROP TABLE IF EXISTS Artist;
        DROP TABLE IF EXISTS Genre;
        DROP TABLE IF EXISTS TrackArtist;
        DROP TABLE IF EXISTS AlbumArtist;
        DROP TABLE IF EXISTS ArtistGenre;
    ''')

def dump_user_saved(cursor, saved_tracks):
    """
    Inserts user saved tracks into the database.
    This function takes a database cursor and a list of saved tracks, and inserts
    the track information into the Track, TrackArtist, Artist, and Album tables.
    If a track or artist already exists in the database, it will be ignored or replaced.
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute SQL commands.
        saved_tracks (list): A list of dictionaries containing track information.
    Each track dictionary should have the following structure:
        {
            'track': {
                'id': str,
                'name': str,
                'artists': [
                    {'id': str, ...},
                    ...
                ],
                'album': {'id': str, ...},
                'duration_ms': int,
                'popularity': int,
                'explicit': bool,
                'track_number': int,
                ...
            }
        }
    """
    for track in saved_tracks:
        track_id = track['track']['id']
        track_name = track['track']['name']
        artist_ids = [artist['id'] for artist in track['track']['artists']]
        album_id = track['track']['album']['id']
        duration = int(track['track']['duration_ms'])
        popularity = int(track['track']['popularity'])
        explicit = int(track['track']['explicit'])
        track_number = int(track['track']['track_number'])

        print(f"Dumping track: {track_name}")

        # Insert into the Track table
        cursor.execute('''
            INSERT OR REPLACE INTO Track (id, name, album_id, duration, popularity, explicit, track_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (track_id, track_name, album_id, duration, popularity, explicit, track_number))

        # Insert into the TrackArtist table and Artist table
        for artist_id in artist_ids:
            cursor.execute('''
                INSERT OR IGNORE INTO TrackArtist (track_id, artist_id)
                VALUES (?, ?)
            ''', (track_id, artist_id))
            cursor.execute('''
                INSERT OR IGNORE INTO Artist (id)
                VALUES (?)
            ''', (artist_id,))
        
        # Insert into the Album table
        album_id = track['track']['album']['id']
        cursor.execute('''
                INSERT OR IGNORE INTO Album (id)
                VALUES (?)
            ''', (album_id,))

def dump_tracks(cursor, tracks):
    """
    Inserts track information into the database.
    This function takes a database cursor and a list of tracks, and inserts
    the track information into the Track, TrackArtist, Artist, and Album tables.
    If a track or artist already exists in the database, it will be ignored or replaced.
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute SQL commands.
        tracks (list): A list of dictionaries containing track information.
    Each track dictionary should have the following structure:
        {
            'id': str,
            'name': str,
            'artists': [
                {'id': str, ...},
                ...
            ],
            'album': {'id': str, ...},
            'duration_ms': int,
            'popularity': int,
            'explicit': bool,
            'track_number': int,
            ...
        }
    """
    for track in tracks:
        track_id = track['id']
        track_name = track['name']     
        artist_ids = [artist['id'] for artist in track['artists']] 
        album_id = track['album']['id']
        duration = int(track['duration_ms'])
        popularity = int(track['popularity'])
        explicit = int(track['explicit'])
        track_number = int(track['track_number'])

        print(f"Dumping track: {track_name}")

        # Insert into the Track table
        cursor.execute('''
            INSERT OR REPLACE INTO Track (id, name, album_id, duration, popularity, explicit, track_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (track_id, track_name, album_id, duration, popularity, explicit, track_number))

        # Insert into the TrackArtist table and Artist table
        for artist_id in artist_ids:
            cursor.execute('''
                INSERT OR IGNORE INTO TrackArtist (track_id, artist_id)
                VALUES (?, ?)
            ''', (track_id, artist_id))
            cursor.execute('''
                INSERT OR IGNORE INTO Artist (id)
                VALUES (?)
            ''', (artist_id,))
        
        # Insert into the Album table
        album_id = track['album']['id']
        cursor.execute('''
                INSERT OR IGNORE INTO Album (id)
                VALUES (?)
            ''', (album_id,))

def dump_albums(cursor, albums):
    """
    Inserts album information into the database.
    This function takes a database cursor and a list of albums, and inserts
    the album information into the Album, AlbumArtist, Artist, and Track tables.
    If an album, artist, or track already exists in the database, it will be ignored or replaced.
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute SQL commands.
        albums (list): A list of dictionaries containing album information.
    Each album dictionary should have the following structure:
        {
            'id': str,
            'name': str,
            'artists': [
                {'id': str, ...},
                ...
            ],
            'release_date': str,
            'total_tracks': int,
            'label': str,
            'album_type': str,
            'popularity': int,
            'tracks': {
                'items': [
                    {'id': str, ...},
                    ...
                ]
            }
        }
    """

    for album in albums:
        album_id = album['id']
        album_name = album['name']
        artist_ids = [artist['id'] for artist in album['artists']]
        release_date = album['release_date']
        total_tracks = album['total_tracks']
        label = album['label']
        album_type = album['album_type']
        popularity = album['popularity']

        # Convert release date to ISO format
        if len(release_date) == 4: release_date = f"{release_date}-01-01"
        elif len(release_date) == 7: release_date = f"{release_date}-01"
        elif len(release_date) == 10: pass
        else: raise ValueError(f"Invalid release date: {release_date}")
            
        print(f"Dumping album: {album_name}")

        # Insert into the Album table
        cursor.execute('''
            INSERT OR REPLACE INTO Album (id, name, release_date, total_tracks, label, album_type, popularity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (album_id, album_name, release_date, total_tracks, label, album_type, popularity))

        # Insert into the AlbumArtist table and Artist table
        for artist_id in artist_ids:
            cursor.execute('''
                INSERT OR IGNORE INTO AlbumArtist (album_id, artist_id)
                VALUES (?, ?)
            ''', (album_id, artist_id))
            cursor.execute('''
                INSERT OR IGNORE INTO Artist (id)
                VALUES (?)
            ''', (artist_id,))
        
        # Insert into the Track table
        for track in album['tracks']['items']:
            track_id = track['id']
            cursor.execute('''
                INSERT OR IGNORE INTO Track (id)
                VALUES (?) 
            ''', (track_id,))

def dump_artists(cursor, artists):
    """
    Inserts artist information into the database.
    This function takes a database cursor and a list of artists, and inserts
    the artist information into the Artist, ArtistGenre, Genre, and Album tables.
    If an artist or genre already exists in the database, it will be ignored or replaced.
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute SQL commands.
        artists (list): A list of dictionaries containing artist information.
    Each artist dictionary should have the following structure:
        {
            'id': str,
            'name': str,
            'popularity': int,
            'followers': {'total': int},
            'genres': [str, ...]
        }
    """
    
    for artist in artists:
        artist_id = artist['id']
        artist_name = artist['name']
        popularity = artist['popularity']
        followers = artist['followers']['total']
        genres = artist['genres']

        print(f"Dumping artist: {artist_name}")

        # Insert into the Artist table
        cursor.execute('''
            INSERT OR REPLACE INTO Artist (id, name, popularity, followers)
            VALUES (?, ?, ?, ?)
        ''', (artist_id, artist_name, popularity, followers))

        # Insert into the ArtistGenre table and Genre table
        for genre in genres:
            cursor.execute('''
                INSERT OR IGNORE INTO Genre (name)
                VALUES (?)
            ''', (genre,))
            cursor.execute('''
                SELECT id FROM Genre WHERE name = ?
            ''', (genre,))
            genre_id = cursor.fetchone()[0]
            cursor.execute('''
                INSERT OR IGNORE INTO ArtistGenre (artist_id, genre_id)
                VALUES (?, ?)
            ''', (artist_id, genre_id))

def dump_artist_albums(cursor, artist_id):
    """
    Inserts album information for a given artist into the database.
    This function takes a database cursor and an artist ID, and inserts
    the album information into the Album, AlbumArtist, Artist, and Track tables.
    If an album, artist, or track already exists in the database, it will be ignored or replaced.
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute SQL commands.
        artist_id (str): The Spotify ID of the artist.
    """
    albums = get_artist_albums(artist_id)

    # Insert into the Album table
    for album in albums:
        album_id = album['id']
        cursor.execute('''
            INSERT OR IGNORE INTO Album (id)
            VALUES (?) 
        ''', (album_id,))

if __name__ == "__main__":
    # Check if logged in, else login
    if not os.path.exists(REFRESH_TOKEN_PATH) or get_user_token() is None: login()

    # Load the request log
    load_request_log()

    # Database loader flow
    # 1. Setup 
    #   a. Create tables 
    #   b. Get user saved tracks info
    #   c. Initial dump of user saved tracks into database
    # 2. Loop
    #   a. Scan database for tracks ids with no info and add to batch
    #   b. Batch request track info, add to database
    #   c. Repeat until all tracks are updated
    #   
    #   a. Scan database for albums ids with no info and add to batch
    #   b. Batch request album info, add to database
    #   c. Repeat until all albums are updated 
    #   
    #   a. Scan database for artists ids with no info and add to batch
    #   b. Batch request artist info, add to database (also get artist's albums) SLOW!!!
    #   c. Repeat until all artists are updated
    # 3. Repeat until all queses are empty

    # Connect to the SQLite database
    os.makedirs("db", exist_ok=True)
    conn = sqlite3.connect("db/spotify.sqlite")
    cursor = conn.cursor()
    
    # Check if running for the first time by checking if tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Track'")
    if cursor.fetchone() is None:
        # Create the tables if they don't exist
        create_tables(cursor)
        conn.commit()
        
        # Initial dump: Get user saved tracks and add to the database
        saved_tracks = get_user_saved()
        dump_user_saved(cursor, saved_tracks)
        conn.commit()

    # Loop until all queues are empty
    # Priority: Tracks -> Albums -> Artists
    # Start at:
    check_type = input("Start at (tracks, albums, artists): ")
    check_albums = input("Check albums? (y/n): ") in ('y', 'yes')
    try:
        while True:
            # Tracks
            i = 1
            while True:
                if check_type != 'tracks': break
                # Scan database for tracks with no info
                cursor.execute('SELECT id FROM Track WHERE name IS NULL LIMIT 50')
                track_ids = [row[0] for row in cursor.fetchall()]

                # Batch request track info and add to database
                if len(track_ids) > 0:
                    track_batch = get_batch_info('track', track_ids)
                    if track_batch is not None: dump_tracks(cursor, track_batch['tracks'])
                else: 
                    conn.commit()
                    print("No tracks to update, moving on...")
                    check_type = 'albums'
                    break
                if i % 20 == 0: 
                    conn.commit() # Commit every 20 batches (1000 tracks)
                    cursor.execute('''SELECT COUNT(id) FROM Track WHERE name IS NULL''')
                    tracks_remaining = cursor.fetchone()[0]
                    print(f"Committing... Tracks remaining: {tracks_remaining}")
                i += 1

            # Albums
            i = 1
            while True:
                if check_type != 'albums': break
                # Scan database for albums with no info
                cursor.execute('SELECT id FROM Album WHERE name IS NULL LIMIT 20')
                album_ids = [row[0] for row in cursor.fetchall()]

                # Batch request album info and add to database
                if len(album_ids) > 0:
                    album_batch = get_batch_info('album', album_ids)
                    if album_batch is not None: dump_albums(cursor, album_batch['albums'])
                else:
                    conn.commit() 
                    print("No albums to update, moving on...")
                    check_type = 'artists'
                    break
                if i % 20 == 0: 
                    conn.commit() # Commit every 20 batches (400 albums)
                    cursor.execute('''SELECT COUNT(id) FROM Album WHERE name IS NULL''')
                    albums_remaining = cursor.fetchone()[0]
                    print(f"Committing... Albums remaining: {albums_remaining}")
                i += 1

            # Artists
            i = 1
            while True:
                if check_type != 'artists': break
                # Scan database for artists with no info
                cursor.execute('SELECT id FROM Artist WHERE name IS NULL LIMIT 50')
                artist_ids = [row[0] for row in cursor.fetchall()]

                # Batch request artist info and add to database
                if len(artist_ids) > 0:
                    artist_batch = get_batch_info('artist', artist_ids)
                    if artist_batch is not None: dump_artists(cursor, artist_batch['artists'])
                else: 
                    conn.commit()
                    print("No artists to update, moving on...")
                    check_type = 'tracks'
                    break
                if i % 1 == 0: 
                    conn.commit() # Commit every 1 batch (50 artists)
                    cursor.execute('''SELECT COUNT(id) FROM Artist WHERE name IS NULL''')
                    artists_remaining = cursor.fetchone()[0]
                    print(f"Committing... Artists remaining: {artists_remaining}")
                i += 1

            # Albums from Artists (resource intensive)
            if check_albums:
                i = 1
                while True:
                    # Scan database for artists whose albums have not been checked yet
                    cursor.execute('SELECT id FROM Artist WHERE retrieved_albums IS 0 LIMIT 50')
                    artist_ids = [row[0] for row in cursor.fetchall()]

                    if len(artist_ids) > 0:
                        for artist_id in artist_ids:
                            dump_artist_albums(cursor, artist_id)
                            cursor.execute('UPDATE Artist SET retrieved_albums = 1 WHERE id = ?', (artist_id,))
                    else: 
                        conn.commit()
                        print("No artists's albums to update, moving on...")
                        check_type = 'tracks'
                        break
                    if i % 1 == 0: 
                        conn.commit()

            # Check type defaults to tracks
            if check_type != 'tracks' and check_type != 'albums' and check_type != 'artists': check_type = 'tracks'
            
            cursor.execute("SELECT COUNT(id) FROM Track WHERE name IS NULL")
            if cursor.fetchone()[0] > 0: continue
                
            cursor.execute("SELECT COUNT(id) FROM Album WHERE name IS NULL")
            if cursor.fetchone()[0] > 0: continue

            cursor.execute("SELECT COUNT(id) FROM Artist WHERE name IS NULL OR retrieved_albums = 0")
            if cursor.fetchone()[0] > 0: continue

            print("All items updated.")
            break
        conn.commit()
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        conn.commit()
        conn.close()
        print("Database connection closed.")
        save_request_log()
        print("Request log saved.")
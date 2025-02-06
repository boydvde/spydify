import os, json, time, sqlite3, requests
from collections import deque
from dotenv import load_dotenv
from spot_access import get_user_token, login

# Load the environment variables
load_dotenv()

REQUEST_LOG_PATH = os.getenv('REQUEST_LOG_PATH')
DEBUG = os.getenv('DEBUG', 'False').lower() in ('1', 'true', 'yes')

# Rate limiting
MAX_REQUESTS_PER_30_SEC = 40 # Max requests per 30 seconds
MAX_REQUESTS_PER_HOUR = 2500 # Max requests per hour
MAX_REQUESTS_PER_DAY = 4500 # Max requests per day

# Global variables to store the timestamps of the requests
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
    global total_requests, halfmin_timestamps, hourly_timestamps, daily_timestamps, response_times, base_wait
    current_time = time.time()

    # Clean old timestamps
    while halfmin_timestamps and current_time - halfmin_timestamps[0] > 30:
        halfmin_timestamps.popleft()
    while hourly_timestamps and current_time - hourly_timestamps[0] > 3600:
        hourly_timestamps.popleft()
    while daily_timestamps and current_time - daily_timestamps[0] > 86400:
        daily_timestamps.popleft()

    if DEBUG and total_requests % 10 == 0:
        print(f"Total requests: {total_requests}")
        print(f"Requests in last 30 seconds: {len(halfmin_timestamps)}")
        print(f"Requests in last hour: {len(hourly_timestamps)}")
        print(f"Requests in last day: {len(daily_timestamps)}")
        print(f"Waiting {base_wait:.2f} seconds before next request...")

    time.sleep(base_wait)  # Base wait time before making requests

    if len(halfmin_timestamps) >= MAX_REQUESTS_PER_30_SEC:
        wait_time = 30 - (current_time - halfmin_timestamps[0])
        print(f"[{time.ctime(current_time)}] Rate limited: Waiting {wait_time:.2f} seconds to avoid 30-sec limit...")
        time.sleep(wait_time + 1)

    if len(hourly_timestamps) >= MAX_REQUESTS_PER_HOUR:
        wait_time = 3600 - (current_time - hourly_timestamps[0])
        print(f"[{time.ctime(current_time)}] Hourly limit reached: Waiting {wait_time / 60:.2f} minutes...")
        time.sleep(wait_time + 1)

    if len(daily_timestamps) >= MAX_REQUESTS_PER_DAY:
        wait_time = 86400 - (current_time - daily_timestamps[0])
        print(f"[{time.ctime(current_time)}] Daily limit reached: Retrying in {wait_time / 3600:.2f} hours...")
        time.sleep(wait_time + 1)

    halfmin_timestamps.append(current_time)
    hourly_timestamps.append(current_time)
    daily_timestamps.append(current_time)
    total_requests += 1

def get_info(item_type, item_id, retries=3):
    """
    Fetches information from the Spotify API for a given item type and ID.

    Args:
        item_type (str): 'track', 'album', 'artist', or 'playlist'.
        item_id (str): The ID of the item.
        retries (int): Number of retries for rate-limited requests.

    Returns:
        dict: JSON response with item information, or None if request fails.
    """
    valid_types = ['track', 'album', 'artist', 'playlist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    url = f'https://api.spotify.com/v1/{item_type}s/{item_id}'
    headers = {'Authorization': f'Bearer {get_user_token()}'}

    for attempt in range(retries):
        check_rate_limit()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < retries - 1:
                retry_after = int(response.headers.get("Retry-After", 1))
                print(f"Rate limited. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                print(f"HTTP Error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        time.sleep(2 ** attempt)  # Exponential backoff
    return None

def get_batch_info(item_type, item_ids, retries=3):
    """
    Fetches batch information for a given item type and list of item IDs from the Spotify API.

    Args:
        item_type (str): 'track', 'album', or 'artist'.
        item_ids (list): List of Spotify item IDs.
        retries (int): Number of retries for rate-limited requests.

    Returns:
        dict: JSON response with batch information, or None if request fails.
    """
    valid_types = ['track', 'album', 'artist']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    if not item_ids:
        return None

    max_sizes = {'track': 50, 'artist': 50, 'album': 20}
    if len(item_ids) > max_sizes[item_type]:
        raise ValueError(f"Max batch size for {item_type}s is {max_sizes[item_type]}")

    url = f'https://api.spotify.com/v1/{item_type}s?ids={",".join(item_ids)}'
    headers = {'Authorization': f'Bearer {get_user_token()}'}

    for attempt in range(retries):
        check_rate_limit()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < retries - 1:
                retry_after = int(response.headers.get("Retry-After", 1))
                print(f"Rate limited. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                print(f"HTTP Error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        time.sleep(2 ** attempt)
    return None

def get_user_saved(retries=3):
    """
    Retrieves the user's saved tracks from Spotify.

    Returns:
        list: List of saved track items from the Spotify API.
    """
    limit = 50
    offset = 0
    total = limit + 1
    items = []

    while offset < total:
        url = f'https://api.spotify.com/v1/me/tracks?limit={limit}&offset={offset}'
        headers = {'Authorization': f'Bearer {get_user_token()}'}
        check_rate_limit()
        
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                total = data['total']
                items.extend(data['items'])
                offset += limit
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429 and attempt < retries - 1:
                    retry_after = int(response.headers.get("Retry-After", 1))
                    print(f"Rate limited. Retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    print(f"HTTP Error: {e}")
                    return items
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
        time.sleep(2 ** attempt)

    return items

def get_artist_albums(artist_id, retries=3):
    """
    Fetches all albums for a given artist from the Spotify API.

    Args:
        artist_id (str): The Spotify ID of the artist.
        retries (int, optional): Number of retries in case of rate limiting. Defaults to 3.

    Returns:
        list: A list of album items for the given artist.
    """
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    
    while offset < total:
        url = f'https://api.spotify.com/v1/artists/{artist_id}/albums'
        params = {
            'limit': limit,
            'offset': offset,
            'include_groups': 'album,single'
        }
        headers = {'Authorization': f'Bearer {get_user_token()}'}
        
        check_rate_limit()
        
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                total = data.get('total', 0)  # Total number of albums
                items.extend(data.get('items', []))  # Add fetched albums to list
                offset += limit  # Increase offset for next batch
                break  # Exit retry loop on success

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429 and attempt < retries - 1:
                    retry_after = int(response.headers.get("Retry-After", 1))
                    print(f"Rate limited. Retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    print(f"HTTP Error: {e}")
                    return items
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

    return items

def create_tables(cursor): # Deprecated (SQL schema changed)
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

def dump_user_saved(conn, cursor, saved_tracks):
    """
    Inserts user saved tracks into the database.
    """
    
    print(f"Dumping {len(saved_tracks)} saved tracks...")

    with conn:
        # Insert into the Track table
        cursor.executemany('''
            INSERT OR REPLACE INTO Track (id, name, album_id, duration, popularity, explicit, track_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', [(track['track']['id'], track['track']['name'], track['track']['album']['id'], int(track['track']['duration_ms']), int(track['track']['popularity']), int(track['track']['explicit']), int(track['track']['track_number']) ) for track in saved_tracks])

        # Insert into the Artist table
        cursor.executemany('''
            INSERT OR IGNORE INTO Artist (id)
            VALUES (?)
        ''', [(artist['id'],) for track in saved_tracks for artist in track['track']['artists']])

        # Insert into the TrackArtist
        cursor.executemany('''
            INSERT OR IGNORE INTO TrackArtist (track_id, artist_id)
            VALUES (?, ?)
        ''', [(track['track']['id'], artist['id']) for track in saved_tracks for artist in track['track']['artists']])
        
        # Insert into the Album table
        cursor.executemany('''
            INSERT OR IGNORE INTO Album (id)
            VALUES (?)
        ''', [(track['track']['album']['id'],) for track in saved_tracks]) 

def dump_tracks(conn, cursor, tracks):
    """
    Inserts track information into the database.
    """

    print(f"Dumping {len(tracks)} tracks...")

    with conn:
        # Insert into the Track table
        cursor.executemany('''
            INSERT OR REPLACE INTO Track (id, name, album_id, duration, popularity, explicit, track_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', [(track['id'], track['name'], track['album']['id'], int(track['duration_ms']), int(track['popularity']), int(track['explicit']), int(track['track_number'])) for track in tracks])
        
        # Insert into the TrackArtist table
        cursor.executemany('''
            INSERT OR IGNORE INTO TrackArtist (track_id, artist_id)
            VALUES (?, ?)
        ''', [(track['id'], artist['id']) for track in tracks for artist in track['artists']])

        # Insert into the Artist table
        cursor.executemany('''
            INSERT OR IGNORE INTO Artist (id)
            VALUES (?)
        ''', [(artist['id'],) for track in tracks for artist in track['artists']])

        # Insert into Album table
        cursor.executemany('''
            INSERT OR IGNORE INTO Album (id) 
            VALUES (?)
        ''', [(track['album']['id'],) for track in tracks])

def dump_albums(conn, cursor, albums):
    """
    Inserts album information into the database.
    """

    print(f"Dumping {len(albums)} albums...")

    with conn:
        # Insert into the Album table
        cursor.executemany('''
            INSERT OR REPLACE INTO Album (id, name, release_date, total_tracks, label, album_type, popularity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', [(album['id'], album['name'], album['release_date'], album['total_tracks'], album['label'], album['album_type'], album['popularity']) for album in albums])
        
        # Insert into the AlbumArtist table 
        cursor.executemany('''
            INSERT OR IGNORE INTO AlbumArtist (album_id, artist_id)
            VALUES (?, ?)
        ''', [(album['id'], artist['id']) for album in albums for artist in album['artists']])

        # Insert into the Artist table
        cursor.executemany('''
            INSERT OR IGNORE INTO Artist (id)
            VALUES (?)
        ''', [(artist['id'],) for album in albums for artist in album['artists']])

        # Insert into the Track table
        cursor.executemany('''
            INSERT OR IGNORE INTO Track (id)
            VALUES (?)
        ''', [(track['id'],) for album in albums for track in album['tracks']['items']])

def dump_artists(conn, cursor, artists):
    """
    Inserts artist information into the database.
    """

    print(f"Dumping {len(artists)} artists...")

    with conn:
        # Insert into the Artist table
        cursor.executemany('''
            INSERT OR REPLACE INTO Artist (id, name, popularity, followers)
            VALUES (?, ?, ?, ?)
        ''', [(artist['id'], artist['name'], artist['popularity'], artist['followers']['total']) for artist in artists])

def dump_artist_albums(cursor, albums):
    """
    Inserts album information for a given artist into the database.
    """

    print(f"Dumping {len(albums)} albums for artist: {artist_id}")

    with conn:
        # Insert into the Album table
        cursor.executemany('''
            INSERT OR IGNORE INTO Album (id)
            VALUES (?)
        ''', [(album['id'],) for album in albums])

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

if __name__ == "__main__":
    # Check if logged in, else login
    if not get_user_token(): login(scope=['user-library-read'])

    # Load the request log
    load_request_log()

    # Connect to the SQLite database
    os.makedirs("db", exist_ok=True)
    conn = sqlite3.connect("db/spotify.sqlite")
    conn.execute("PRAGMA journal_mode=WAL")  # Enable Write-Ahead Logging for better concurrency
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
    check_type = input("Start at (tracks, albums, artists): ")
    check_albums = input("Check albums? (y/n): ") in ('y', 'yes')
    try:
        while True:
            # Tracks
            i = 1
            while True:
                if check_type != 'tracks': break
                # Scan database for tracks with no info
                cursor.execute('SELECT id FROM Track WHERE name IS NULL ORDER BY RANDOM() LIMIT 50;')
                track_ids = [row[0] for row in cursor.fetchall()]

                # Batch request track info and add to database
                if len(track_ids) > 0:
                    track_batch = get_batch_info('track', track_ids)
                    if track_batch is not None: dump_tracks(cursor, track_batch['tracks']); conn.commit()
                else: 
                    print("No tracks to update, moving on...")
                    check_type = 'albums'
                    break
                if i % 10 == 0: # Print progress every 10 batches
                    cursor.execute('''SELECT COUNT(id) FROM Track WHERE name IS NULL''')
                    tracks_remaining = cursor.fetchone()[0]
                    print(f"Tracks remaining: {tracks_remaining}")
                i += 1

            # Albums
            i = 1
            while True:
                if check_type != 'albums': break
                # Scan database for albums with no info
                cursor.execute('SELECT id FROM Album WHERE name IS NULL ORDER BY RANDOM() LIMIT 20;')
                album_ids = [row[0] for row in cursor.fetchall()]

                # Batch request album info and add to database
                if len(album_ids) > 0:
                    album_batch = get_batch_info('album', album_ids)
                    if album_batch is not None: dump_albums(cursor, album_batch['albums']); conn.commit()
                else:
                    print("No albums to update, moving on...")
                    check_type = 'artists'
                    break
                if i % 10 == 0: # Print progress every 10 batches
                    cursor.execute('''SELECT COUNT(id) FROM Album WHERE name IS NULL''')
                    albums_remaining = cursor.fetchone()[0]
                    print(f"Albums remaining: {albums_remaining}")
                i += 1

            # Artists
            i = 1
            while True:
                if check_type != 'artists': break
                # Scan database for artists with no info
                cursor.execute('SELECT id FROM Artist WHERE name IS NULL ORDER BY RANDOM() LIMIT 50;')
                artist_ids = [row[0] for row in cursor.fetchall()]

                # Batch request artist info and add to database
                if len(artist_ids) > 0:
                    artist_batch = get_batch_info('artist', artist_ids)
                    if artist_batch is not None: dump_artists(cursor, artist_batch['artists']); conn.commit()
                else: 
                    print("No artists to update, moving on...")
                    check_type = 'tracks'
                    break
                if i % 10 == 0: # Print progress every 10 batches
                    cursor.execute('''SELECT COUNT(id) FROM Artist WHERE name IS NULL''')
                    artists_remaining = cursor.fetchone()[0]
                    print(f"Artists remaining: {artists_remaining}")
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
                            albums = get_artist_albums(artist_id)
                            dump_artist_albums(cursor, albums)
                            cursor.execute('UPDATE Artist SET retrieved_albums = 1 WHERE id = ?', (artist_id,))
                            conn.commit()
                    else: 
                        print("No artists's albums to update, moving on...")
                        check_type = 'tracks'
                        break
                    if i % 2 == 0: 
                        cursor.execute('''SELECT COUNT(id) FROM Artist WHERE retrieved_albums IS 0''')
                        artists_remaining = cursor.fetchone()[0]
                        print(f"Artists remaining: {artists_remaining}")
                    i += 1

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
        save_request_log()
        print("Database connection closed.")
        print("Request log saved.")
import os, ssl, json, time, datetime, sqlite3
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

DEBUG = True

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
    
def get_user_saved():
    limit = 50
    offset = 0
    total = limit + 1
    items = []
    while offset < total:
        req = urllib.request.Request(f'https://api.spotify.com/v1/me/tracks?limit={limit}&offset={offset}', method="GET")
        req.add_header('Authorization', f'Bearer {get_token()}')
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

def create_tables(cursor):
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
    cursor.execute('DROP TABLE IF EXISTS Track')
    cursor.execute('DROP TABLE IF EXISTS Album')
    cursor.execute('DROP TABLE IF EXISTS Artist')
    cursor.execute('DROP TABLE IF EXISTS Genre')
    cursor.execute('DROP TABLE IF EXISTS TrackArtist')
    cursor.execute('DROP TABLE IF EXISTS AlbumArtist')
    cursor.execute('DROP TABLE IF EXISTS ArtistGenre')

def dump_user_saved(cursor, saved_tracks):
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
    for album in albums:
        album_id = album['id']
        album_name = album['name']
        artist_ids = [artist['id'] for artist in album['artists']]
        release_date = album['release_date']
        total_tracks = album['total_tracks']
        label = album['label']
        album_type = album['album_type']
        popularity = album['popularity']
            
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
                INSERT OR IGNORE INTO ArtistGenre (artist_id, genre_id)
                VALUES (?, ?)
            ''', (artist_id, cursor.lastrowid))

if __name__ == "__main__":
    # Check if logged in, else login
    if not os.path.exists(REFRESH_TOKEN_PATH) or get_token() is None: login()

    # Database loader flow
    # 1. Setup
    #    a. Create tables 
    #    b. Get user saved tracks info
    #    c. Initial dump of user saved tracks into database
    # 2. Loop
    #    a. Scan database for albums ids with no info and add to batch
    #    b. Batch request album info, add to database
    #  
    #    c. Scan database for artists ids with no info and add to batch
    #    d. Batch request artist info, add to database
    #
    #    e. Scan database for tracks ids with no info and add to batch
    #    f. Batch request track info, add to database
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
    while True:
        # Scan database for albums with no info
        cursor.execute('SELECT id FROM Album WHERE name IS NULL LIMIT 20')
        album_ids = [row[0] for row in cursor.fetchall()]

        # Batch request album info and add to database
        if len(album_ids) > 0:
            album_batch = get_batch_info('album', album_ids)
            if album_batch is not None: dump_albums(cursor, album_batch['albums'])
        else: print("No albums to update")
        conn.commit()

        # Scan database for artists with no info
        cursor.execute('SELECT id FROM Artist WHERE name IS NULL LIMIT 50')
        artist_ids = [row[0] for row in cursor.fetchall()]

        # Batch request artist info and add to database
        if len(artist_ids) > 0:
            artist_batch = get_batch_info('artist', artist_ids)
            if artist_batch is not None: dump_artists(cursor, artist_batch['artists'])
        else: print("No artists to update")
        conn.commit()

        # Scan database for tracks with no info
        cursor.execute('SELECT id FROM Track WHERE name IS NULL LIMIT 50')
        track_ids = [row[0] for row in cursor.fetchall()]

        # Batch request track info and add to database
        if len(track_ids) > 0:
            track_batch = get_batch_info('track', track_ids)
            if track_batch is not None: dump_tracks(cursor, track_batch['tracks'])
        else: print("No tracks to update")
        conn.commit()

        # Break if all queues are empty
        if len(album_ids) == 0 and len(artist_ids) == 0 and len(track_ids) == 0:
            print("All queues are empty, exiting...")
            break
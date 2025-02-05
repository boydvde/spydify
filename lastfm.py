import requests, sqlite3, os, time, concurrent.futures
from collections import deque
from dotenv import load_dotenv

load_dotenv()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
timestamps = deque()

def get_genre(artist_name):
    """
    Fetches artist genres from Last.fm.
    """
    # Rate limit to 5 requests per second
    global timestamps
    cur_time = time.time()
    while timestamps and cur_time - timestamps[0] > 1: timestamps.popleft()
    if len(timestamps) >= 5: time.sleep(1 - (cur_time - timestamps[0]))

    # Fetch genres from Last.fm
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={LASTFM_API_KEY}&format=json"
    response = requests.get(url)
    try: 
        data = response.json()
    except requests.JSONDecodeError:
        print(f"Failed to decode JSON for {artist_name}")
        return None
    timestamps.append(time.time())
    if "artist" in data and "tags" in data["artist"]:
        genres = [tag["name"] for tag in data["artist"]["tags"]["tag"]]
        if genres:
            return genres 
    return ['unknown']

def fetch_genres_parallel(artist_list):
    """
    Fetches genres for multiple artists in parallel using ThreadPoolExecutor.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:  # Max 5 requests/sec
        results = executor.map(get_genre, artist_list)

    return list(results)  # Convert map object to list

def save_genres_to_db(cursor, artist_data, genre_data):
    """
    Bulk inserts genres and artist-genre relationships into SQLite.
    """
    genre_insert = set()
    artist_genre_insert = [] 

    for (artist_id, _), genres in zip(artist_data, genre_data): # (abc123, _), ['electronic', 'house']
        for genre in genres:
            genre_insert.add((genre,)) 
            artist_genre_insert.append((artist_id, genre)) # [(abc123, electronic), (abc123, house), ...]

    # Insert all genres in bulk
    cursor.executemany("INSERT OR IGNORE INTO Genre (name) VALUES (?)", genre_insert)

    # Insert artist-genre relationships in bulk
    cursor.executemany("""
        INSERT OR IGNORE INTO ArtistGenre (artist_id, genre_id)
        VALUES (?, (SELECT id FROM Genre WHERE name = ?))
    """, artist_genre_insert)

if __name__ == "__main__":
    try:
        # Connect to SQLite database
        conn = sqlite3.connect("db/spotify.sqlite")
        cursor = conn.cursor()

        # Count artists without genres
        cursor.execute(
            """
            SELECT COUNT(*) AS artists_without_genres
            FROM Artist
            WHERE id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre);
            """)
        total = cursor.fetchone()[0]

        # Fetch all artists
        cursor.execute("""
            SELECT id, name FROM Artist 
            WHERE id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre)
        """)
        all_artists = cursor.fetchall()

        batch_size = 50  # Process 50 artists at a time
        for i in range(0, len(all_artists), batch_size):
            artist_batch = all_artists[i : (i + batch_size)]
            artist_names = [name for _, name in artist_batch]

            # Fetch genres in parallel
            genre_results = fetch_genres_parallel(artist_names)

            # Save results to database
            save_genres_to_db(cursor, artist_batch, genre_results)
            conn.commit()

            print(f"Processed {i + batch_size}/{total} artists")

        conn.close()
        print("All artist genres updated!")
    
    except KeyboardInterrupt:
        conn.commit()
        conn.close()
        print("Process interrupted. Progress saved.")
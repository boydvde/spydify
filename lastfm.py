import requests, sqlite3, os, time, concurrent.futures
from collections import deque
from dotenv import load_dotenv

# Load API Key
load_dotenv()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
timestamps = deque()

def get_genre(artist_name, retries=3):
    """
    Fetches artist genres from Last.fm.
    Retries up to 3 times if API returns invalid JSON or fails.
    """
    global timestamps
    cur_time = time.time()

    while timestamps and cur_time - timestamps[0] > 1:
        timestamps.popleft()
    
    if len(timestamps) >= 5:
        time.sleep(1 - (cur_time - timestamps[0]))

    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.getTopTags",
        "artist": artist_name,
        "api_key": LASTFM_API_KEY,
        "format": "json"
    }

    for attempt in range(retries):
        try:
            response = requests.get(url=url, params=params)
            response.raise_for_status()  # Raises error for HTTP failures (4xx, 5xx)

            try:
                data = response.json()

                # {
                #     "toptags": {
                #         "tag": [
                #             {"name": "rock", "count": 100},
                #             {"name": "alternative", "count": 85},
                #             {"name": "indie", "count": 75}
                #         ]
                #     }
                # }

            except requests.exceptions.JSONDecodeError:
                print(f"JSONDecodeError for artist {artist_name}. Retrying...")
                time.sleep(2)
                continue  # Retry

            timestamps.append(time.time())

            if data.get("toptags", {}).get("tag"): # Check if tag list is not empty
                return [data["toptags"]["tag"][0]["name"]] # Return the most popular tag
            return ["unknown"] # Return "unknown" if no tags found
        
        except requests.RequestException as e:
            print(f"⚠️ API request failed for {artist_name}: {e}")
            if attempt < retries - 1:
                time.sleep(2)  # Wait before retrying
            else:
                print(f"❌ Failed after {retries} attempts.")
                return ["unknown"]

def fetch_genres_parallel(artist_list):
    """Fetches genres for multiple artists in parallel using ThreadPoolExecutor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(get_genre, artist_list)
    return list(results)

def save_genres_to_db(cursor, artist_data, genre_data):
    """Bulk inserts genres and artist-genre relationships into SQLite."""
    genre_insert = set()
    artist_genre_insert = []

    for (artist_id, _), genres in zip(artist_data, genre_data):
        for genre in genres:
            genre_insert.add((genre,))
            artist_genre_insert.append((artist_id, genre))

    cursor.executemany("INSERT OR IGNORE INTO Genre (name) VALUES (?)", genre_insert)
    cursor.executemany("""
        INSERT OR IGNORE INTO ArtistGenre (artist_id, genre_id)
        VALUES (?, (SELECT id FROM Genre WHERE name = ?))
    """, artist_genre_insert)

if __name__ == "__main__":
    try:
        conn = sqlite3.connect("db/spotify.sqlite")
        cursor = conn.cursor()

        while True:
            cursor.execute("""
                SELECT id, name FROM Artist 
                WHERE id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre)
                LIMIT 50;
            """)
            artist_batch = cursor.fetchall()

            if not artist_batch:
                print("✅ All artist genres updated!")
                break

            artist_names = [name for _, name in artist_batch]
            genre_results = fetch_genres_parallel(artist_names)

            save_genres_to_db(cursor, artist_batch, genre_results)
            conn.commit()

            cursor.execute("""
                SELECT COUNT(*) FROM Artist
                WHERE id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre);
            """)
            total = cursor.fetchone()[0]

            print(f"Processed {len(artist_batch)} artists, {total} remaining.")

        conn.close()
    
    except KeyboardInterrupt:
        conn.commit()
        conn.close()
        print("Process interrupted. Progress saved.")

import requests
import sqlite3
import time
import concurrent.futures
from collections import deque

timestamps = deque()

def get_artist_data_batch(artist_names, retries=3):
    """
    Fetches country and genre data for multiple artists in a single MusicBrainz request.
    """
    global timestamps
    cur_time = time.time()

    while timestamps and cur_time - timestamps[0] > 1:
        timestamps.popleft()

    if len(timestamps) >= 1:
        time.sleep(1 - (cur_time - timestamps[0]))

    query = " OR ".join([f'"{name}"' for name in artist_names])
    url = f"https://musicbrainz.org/ws/2/artist/"
    params = {
        "query": query,
        "fmt": "json",
        "limit": 100,
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            artist_info = {}
            for artist in data.get("artists", []):
                name = artist.get("name", "Unknown")
                country = artist.get("country", "Unknown")
                tags = [tag["name"] for tag in artist.get("tags", [])] if "tags" in artist else ["unknown"]
                artist_info[name] = (country, tags)

            return artist_info
        
        except requests.RequestException as e:
            print(f"⚠️ API request failed: {e}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return {name: ("Unknown", ["unknown"]) for name in artist_names}

def fetch_artist_data_parallel(artist_list):
    """Fetches country and genre data for multiple artists in parallel."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(get_artist_data_batch, [artist_list[i:i+100] for i in range(0, len(artist_list), 100)])
    
    artist_data = {}
    for batch in results:
        artist_data.update(batch)
    
    return artist_data

def save_artist_data_to_db(cursor, artist_data, fetched_data):
    """Saves artist country and genre data into SQLite in bulk."""
    genre_insert = set()
    artist_genre_insert = []

    for (artist_id, artist_name) in artist_data:
        country, genres = fetched_data.get(artist_name, ("Unknown", ["unknown"]))
        cursor.execute("UPDATE Artist SET country = ? WHERE id = ?", (country, artist_id))

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
                WHERE country IS NULL OR id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre)
                LIMIT 100;
            """)
            artist_batch = cursor.fetchall()

            if not artist_batch:
                print("✅ All artist data updated!")
                break

            artist_names = [name for _, name in artist_batch]
            fetched_results = fetch_artist_data_parallel(artist_names)

            save_artist_data_to_db(cursor, artist_batch, fetched_results)
            conn.commit()

            cursor.execute("""
                SELECT COUNT(*) FROM Artist
                WHERE country IS NULL OR id NOT IN (SELECT DISTINCT artist_id FROM ArtistGenre);
            """)
            total = cursor.fetchone()[0]

            print(f"Processed {len(artist_batch)} artists, {total} remaining.")

        conn.close()
    
    except KeyboardInterrupt:
        conn.commit()
        conn.close()
        print("Process interrupted. Progress saved.")

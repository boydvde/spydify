import requests
import sqlite3
import time
import random
from collections import deque

BATCH_SIZE = 100
timestamps = deque()

def get_artist_data_batch(artist_names, retries=5):
    """
    Fetch artist data from MusicBrainz API one by one instead of in a batch.
    """
    global timestamps
    organized_data = {}

    url = "https://musicbrainz.org/ws/2/artist/"
    headers = {"User-Agent": "WIPArtistMapApp/1.0 (boydbenjamin@live.com)"}

    for artist_name in artist_names:
        cur_time = time.time()

        # Ensure rate limit (1 request per second)
        while timestamps and cur_time - timestamps[0] > 1:
            timestamps.popleft()
        if len(timestamps) >= 1:
            time.sleep(1 - (cur_time - timestamps[0]))

        params = {"query": f'artist:"{artist_name}"', "fmt": "json", "limit": 5}  # Fetch a few results

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                timestamps.append(time.time())
                data = response.json()

                # Filter for exact match
                for artist in data.get("artists", []):
                    if artist["name"].lower() == artist_name.lower():  # Ensure exact match
                        area_info = artist.get("area") or artist.get("begin-area")
                        area_name = area_info.get("name", "Unknown") if area_info else "Unknown"
                        area_type = area_info.get("type", "Unknown") if area_info else "Unknown"
                        genres = [tag["name"] for tag in artist.get("tags", [])] if artist.get("tags") else ['Unknown']

                        print(f"Extracted for {artist_name}: ({area_name}, {area_type}), {genres}")
                        organized_data[artist_name] = ((area_name, area_type), genres)
                        break  # Stop checking after finding an exact match

                if artist_name not in organized_data:
                    print(f"Exact match not found for {artist_name}.")

                break  # Exit retry loop on success

            except requests.exceptions.RequestException as e:
                print(f"API request failed for {artist_name}: {e}")
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)

    # Log missing artists
    missing_artists = set(artist_names) - set(organized_data.keys())
    if missing_artists:
        print(f"Missing artists in response: {missing_artists}")

    return organized_data

def save_artist_data_to_db(cursor, artist_data, fetched_data):
    """
    Saves artist area and genre data into SQLite in bulk.
    """
    for (artist_id, artist_name) in artist_data:
        area, genres = fetched_data.get(artist_name, (("Unknown", "Unknown"), ['Unknown']))  # Kind of redundant, but just in case

        # Insert the area if it does not exist, then fetch the ID
        cursor.execute("INSERT OR IGNORE INTO Area (name, type) VALUES (?, ?)", (area[0], area[1]))
        cursor.execute("SELECT id FROM Area WHERE name = ? AND type = ?", (area[0], area[1]))
        area_row = cursor.fetchone()
        if area_row is None:
            print(f"Failed to fetch area_id for area: {area}")
            continue
        area_id = area_row[0]

        # Update the Artist table with the area_id
        cursor.execute("UPDATE Artist SET area_id = ? WHERE id = ?", (area_id, artist_id))

        # Insert genres
        for genre in genres:
            cursor.execute("INSERT OR IGNORE INTO Genre (name) VALUES (?)", (genre,))
            cursor.execute("SELECT id FROM Genre WHERE name = ?", (genre,))
            genre_row = cursor.fetchone()
            if genre_row is None:
                print(f"Failed to fetch genre_id for genre: {genre}")
                continue
            genre_id = genre_row[0]

            # Insert artist-genre relationships
            cursor.execute("INSERT OR IGNORE INTO ArtistGenre (artist_id, genre_id) VALUES (?, ?)", (artist_id, genre_id))

if __name__ == "__main__":
    try:
        conn = sqlite3.connect("db/spotify.sqlite")
        cursor = conn.cursor()

        while True:
            # SELECT artists without area or genre data
            cursor.execute("""
                SELECT A.id, A.name
                FROM Artist A
                LEFT JOIN ArtistGenre AG ON A.id = AG.artist_id
                WHERE (A.area_id IS NULL OR AG.artist_id IS NULL) 
                AND A.name IS NOT NULL
                LIMIT ?;
            """, (BATCH_SIZE,))
            artist_batch = cursor.fetchall()

            # Exit if no more artists to process
            if not artist_batch:
                print("All artist data updated!")
                break

            artist_names = [name for _, name in artist_batch]  # Extract artist names for lookup
            fetched_results = get_artist_data_batch(artist_names)  # Fetch data from MusicBrainz

            save_artist_data_to_db(cursor, artist_batch, fetched_results)
            conn.commit()

            # Print progress
            cursor.execute("""
                SELECT COUNT(A.id)
                FROM Artist A
                LEFT JOIN ArtistGenre AG ON A.id = AG.artist_id
                WHERE (A.area_id IS NULL OR AG.artist_id IS NULL) 
                AND A.name IS NOT NULL;
            """)
            total = cursor.fetchone()[0]
            print(f"Processed {len(artist_batch)} artists, {total} remaining.")

    except KeyboardInterrupt:
        print("Process interrupted. Progress saved.")
    
    finally:
        conn.commit()
        conn.close()

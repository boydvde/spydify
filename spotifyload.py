import urllib.request, urllib.error, urllib.parse
import os, time, ssl, json, webbrowser, base64
from dotenv import load_dotenv

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

def user_auth(scope: list=None):
    """
    Request user authorization in the web browser.

    Args:
        scope (list, optional): A list of scopes for the authorization. Defaults to an empty list.
    """

    if scope is None:
        scope = []
    endpoint = 'https://accounts.spotify.com/authorize?'
    params = urllib.parse.urlencode({
        'client_id': client_id,
        'response_type': 'code',
        'redirect_uri': redirect_uri, 
        'scope': ' '.join(scope)
    })
    webbrowser.open(f'{endpoint}{params}')

def fetch_auth_code():
    url = "http://localhost:3000/auth_code"
    while True:
        print("Fetching auth code...")
        try:
            with urllib.request.urlopen(url) as response:
                print("Response received.")
                data = json.loads(response.read().decode())
                if data["auth_code"]:
                    return data["auth_code"]
        except Exception as e:
            print(f"Error fetching auth code: {e}")
        time.sleep(1)

def exchange_auth_code(code: str):
    """
    Exchange the authorization code for an access token.

    Args:
        code (str): The authorization code received from Spotify.

    Returns:
        dict: The JSON response containing the access token and refresh token.
    """

    # Create a request to exchange the authorization code for an access token
    data = urllib.parse.urlencode({
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri,
    }).encode()
    req = urllib.request.Request('https://accounts.spotify.com/api/token', data=data, method="POST")
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    req.add_header('Authorization', 'Basic ' + base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode())
    
    # Retrieve the response from the server
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            js = json.loads(content)
    except urllib.error.HTTPError as e:
        print(f"Failed to retrieve response from {req.full_url}: {e.code} {e.reason}")
        return {}
    
    os.makedirs("temp", exist_ok=True)
    # Save the access token to a file if it exists
    if 'access_token' in js:
        with open(ACCESS_TOKEN_PATH, "w") as access_token_file:
            access_token_file.write(js['access_token'])
    else:
        print("Access token not found in the response")

    # Save the refresh token to a file if it exists
    if 'refresh_token' in js:
        with open(REFRESH_TOKEN_PATH, "w") as refresh_token_file:
            refresh_token_file.write(js['refresh_token'])
    else:
        print("Refresh token not found in the response")

    return js # Return the JSON response for debugging

def get_token():
    # TODO: Fix 401 error when token is expired. (age > 3600)
    """
    Retrieve the access token, either from a file if it exists and is valid, or by refreshing it using the refresh token.

    Returns:
        str: The access token if retrieval is successful.
        None: If the token retrieval fails.
    Raises:
        FileNotFoundError: If the refresh token file does not exist.
    """
    # Check if the access token exists and is less than an hour old
    if os.path.exists(ACCESS_TOKEN_PATH):
        token_age = time.time() - os.path.getmtime(ACCESS_TOKEN_PATH) # seconds
        print(f"Token age: {token_age} seconds.")
        if token_age < 3600:
            with open(ACCESS_TOKEN_PATH, "r") as access_token_file:
                return access_token_file.readline().strip()

    # Else refresh the token
    if not os.path.exists(REFRESH_TOKEN_PATH):
        raise FileNotFoundError("Refresh token not found")

    # Read the refresh token from a file
    with open(REFRESH_TOKEN_PATH, "r") as refresh_token_file:
        refresh_token = refresh_token_file.read()

    # Create a request to refresh the access token
    data = urllib.parse.urlencode({
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }).encode()
    req = urllib.request.Request('https://accounts.spotify.com/api/token', data=data, method="POST")
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    req.add_header('Authorization', 'Basic ' + base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode())

    try:
        # Retrieve the response
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            js = json.loads(content)
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code} {e.reason}")
        return None

    # Save the access token to a file if it exists
    if 'access_token' in js:
        with open(ACCESS_TOKEN_PATH, "w") as access_token_file:
            access_token_file.write(js['access_token'])
    else:
        print("Access token not found in the response")
        return None

def login():
    user_auth(['user-library-read'])
    print("Please authorize the application in the web browser.")
    print("Waiting for authorization...")
    auth_code = fetch_auth_code()
    exchange_auth_code(auth_code)
    print("Authorization successful.")

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
        ValueError: If the item_type is not one of 'tracks', 'albums', 'artists' or 'playlists'.
    """
    valid_types = ['tracks', 'albums', 'artists', 'playlists']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}/{item_id}', method="GET")
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
    
    if len(item_ids) == 0:
        return None
    elif len(item_ids) > 50:
        raise ValueError("Maximum number of items is 50")
    
    data = urllib.parse.urlencode({'ids': ','.join(item_ids)}).encode()
    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}', data=data, method="GET")
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

# Necessary scopes for the application: user-library-read
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
        # Get user saved tracks, print them to the console, and write them to a file
        with open('temp/saved_tracks.txt', 'w', encoding='utf-8') as file: # Clear the file
            file.write("User Saved Tracks\n")
        with open('temp/saved_tracks.txt', 'a', encoding='utf-8') as file: # Append to the file
            for track in get_user_saved(get_token()):
                print(f"{track['track']['name']} by {track['track']['artists'][0]['name']}")
                file.write(f"{track['track']['name']} by {track['track']['artists'][0]['name']}\n")
    
    # Get info (track/album/artist/playlist)
    elif choice == '2':
        c = 1
        # Get track/album/artist info
        item_type = input("Enter the item type (tracks, albums, artists or playlists): ")
        item_id = input("Enter the Spotify ID: ")
        info = get_info(item_type, item_id)
        if info is not None:
            if item_type == 'tracks':
                print(f"Track: {info['name']} by {info['artists'][0]['name']}") # Print track info
            elif item_type == 'albums':
                print(f"Album: {info['name']} by {info['artists'][0]['name']}") # Print album info
                for track in info['tracks']['items']:
                    print(f"{c}: {track['name']}") # Print name of each track in the album 
                    c += 1
            elif item_type == 'artists':
                print(f"Artist: {info['name']}") # Print artist info
            elif item_type == 'playlists':
                print(f"\nPlaylist: {info['name']} by {info['owner']['display_name']}") # Print playlist info
                for track in info['tracks']['items']:
                    print(f"{c}: {track['track']['name']} by {track['track']['artists'][0]['name']}") # Print name and artist of each track in the playlist
                    c += 1
    
    elif choice == '3':
        print('WIP')
    
    # Exit
    else:
        print("Invalid choice. Exiting...")
        exit(0)


'''
Spider logic 
1. Get user saved tracks
2. Get all item types + spotify ID
3. Add to queue
4. Get info for each item in the queue
5. Add to queue if not visited
6. Repeat until queue is empty
'''
import urllib.request, urllib.error, urllib.parse
import os, time, ssl, json, webbrowser, base64
from dotenv import load_dotenv

# Load the environment variables 
load_dotenv()
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')

# Define file paths as constants
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
    except urllib.error.URLError as e:
        print(f"Failed to retrieve response from {req.full_url}: {e.reason}")
        with open("temp/error_log.txt", "a") as error_log:
            error_log.write(f"Failed to retrieve response from {req.full_url}: {e.reason}\n")
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
        time_diff = time.time() - os.path.getmtime(ACCESS_TOKEN_PATH)
        print(f"Time difference: {time_diff}")
        if time_diff < 3600:
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
    except urllib.error.URLError as e:
        print(f"Failed to retrieve response: {e.reason}")
        return None

    # Save the access token to a file if it exists
    if 'access_token' in js:
        with open(ACCESS_TOKEN_PATH, "w") as access_token_file:
            access_token_file.write(js['access_token'])
    else:
        print("Access token not found in the response")
        return None

def get_spotify_info(item_type, item_id, token, retries=3):
    """
    Retrieve information from Spotify for a specific item type and ID.

    Args:
        item_type (str): The type of item to retrieve. Must be one of 'tracks', 'albums', or 'artists'.
        item_id (str): The unique identifier for the item.
        token (str): The access token for Spotify API authentication.

    Returns:
        dict: The information retrieved from Spotify for the specified item.

    Raises:
        ValueError: If the item_type is not one of 'tracks', 'albums', or 'artists'.
    """
    valid_types = ['tracks', 'albums', 'artists']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}/{item_id}', method="GET")
    req.add_header('Authorization', f'Bearer {token}')
    try:
        with urllib.request.urlopen(req) as r:
            content = r.read().decode()
            return json.loads(content)
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get('Retry-After', 1))
            print(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return get_spotify_info(item_type, item_id, token, retries - 1)
        print(f"HTTPError: {e.code} - {e.reason}")
    except urllib.error.URLError as e:
        print(f"URLError: {e.reason}")
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
            print(f"HTTPError: {e.code} - {e.reason}")
            break
        except urllib.error.URLError as e:
            print(f"URLError: {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return items

def login():
    user_auth(['user-library-read'])
    input("Press Enter after authorizing the application...")
    with open("temp/auth_token", "r") as file:
        auth_code = file.read()
    exchange_auth_code(auth_code)

# Necessary scopes for the application: user-library-read
if __name__ == "__main__":
    # Check if the refresh token exists, and if not, login
    if not os.path.exists(REFRESH_TOKEN_PATH):    login()

    # Get user saved tracks and write to a file
    with open("json/tracks.json", "w") as file:
        file.write(json.dumps(get_user_saved(get_token()), indent=4))

    with open("json/tracks.json", "r") as file:
        tracks = json.load(file)

    for track in tracks:
        print(f"{track['track']['name']} by {track['track']['artists'][0]['name']}")


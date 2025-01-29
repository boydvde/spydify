import urllib.request, urllib.error, urllib.parse
import os, time, ssl, json, webbrowser, base64
from dotenv import load_dotenv

# Load the environment variables 
load_dotenv()
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')

# Create an SSL context to ignore certificate verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Function to request user authorization in web browser
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

# Function to exchange authorization code for access token, only called after user authorization
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
        return {}
    
    os.makedirs("temp", exist_ok=True)
    # Save the access token to a file
    with open("temp/access_token", "w") as access_token_file:
        access_token_file.write(js['access_token'])

    with open("temp/refresh_token", "w") as refresh_token_file:
        refresh_token_file.write(js['refresh_token'])

    return js # Return the JSON response for debugging

# Function to get the access token, either from a file or by refreshing the token
def get_token():
    # Check if the access token exists and is less than an hour old
    if os.path.exists("temp/access_token"):
        time_diff = time.time() - os.path.getmtime("temp/access_token")
        print(f"Time difference: {time_diff}")
        if time_diff < 3600:
            return open("temp/access_token", "r").read()

    # Else refresh the token
    if not os.path.exists("temp/refresh_token"):
        raise FileNotFoundError("Refresh token not found")

    # Read the refresh token from a file
    with open("temp/refresh_token", "r") as refresh_token_file:
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

    # Save the access token to a file
    with open("temp/access_token", "w") as access_token_file:
        access_token_file.write(js['access_token']) 

# Function to get information from Spotify (tracks, albums, or artists)
def get_spotify_info(item_type, item_id, token):
    valid_types = ['tracks', 'albums', 'artists']
    if item_type not in valid_types:
        raise ValueError(f"Invalid item_type. Expected one of {valid_types}")

    req = urllib.request.Request(f'https://api.spotify.com/v1/{item_type}/{item_id}', method="GET")
    req.add_header('Authorization', f'Bearer {token}')
    with urllib.request.urlopen(req) as r:
        content = r.read().decode()
        return json.loads(content)
    
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
login()

# Get user saved tracks and write to a file
with open("json/tracks.json", "w") as file:
    file.write(json.dumps(get_user_saved(get_token()), indent=4))

with open("json/tracks.json", "r") as file:
    tracks = json.load(file)

for track in tracks:
    print(f"{track['track']['name']} by {track['track']['artists'][0]['name']}")


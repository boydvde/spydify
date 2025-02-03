import os, time, ssl, json, webbrowser, base64
import urllib.request, urllib.error, urllib.parse
from dotenv import load_dotenv

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
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI, 
        'scope': ' '.join(scope)
    })
    webbrowser.open(f'{endpoint}{params}')

def fetch_auth_code():
    """
    Continuously fetches the authorization code from the local server.

    Returns:
        str: The authorization code received from the local server.
    """
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
    Exchange the authorization code for an access token and save the tokens to files.

    Args:
        code (str): The authorization code received from Spotify.

    Returns:
        dict: The JSON response containing the access token and refresh token.
    """

    # Create a request to exchange the authorization code for an access token
    data = urllib.parse.urlencode({
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
    }).encode()
    req = urllib.request.Request('https://accounts.spotify.com/api/token', data=data, method="POST")
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    req.add_header('Authorization', 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode())
    
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
        # print(f"Token age: {token_age} seconds.")
        if token_age < 3540: # 59 minutes
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
    req.add_header('Authorization', 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode())

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
            return js['access_token']
    else:
        print("Access token not found in the response")
        return None

def login():
    """
    Requests user authorization and exchanges the authorization code for access and refresh tokens.
    """
    user_auth(['user-library-read'])
    print("Please authorize the application in the web browser.")
    print("Waiting for authorization...")
    auth_code = fetch_auth_code()
    exchange_auth_code(auth_code)
    print("Authorization successful.")

if __name__ == "__main__":
    # Check if logged in, else login
    if not os.path.exists(REFRESH_TOKEN_PATH) or get_token() is None: login()
    else: print("Already logged in.")
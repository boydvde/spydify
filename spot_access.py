import os, time, requests, webbrowser, base64
from dotenv import load_dotenv

# Load the environment variables
load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
ACCESS_TOKEN_PATH = os.getenv('ACCESS_TOKEN_PATH')
REFRESH_TOKEN_PATH = os.getenv('REFRESH_TOKEN_PATH')
SERVER_TOKEN_PATH = os.getenv('SERVER_TOKEN_PATH')
DEBUG = os.getenv('DEBUG', 'False').lower() in ('1', 'true', 'yes')

print("Debug mode:", DEBUG)

def user_auth(scope=None):
    """
    Request user authorization in the web browser.
    """
    if scope is None:
        scope = []
    endpoint = 'https://accounts.spotify.com/authorize'
    params = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI, 
        'scope': ' '.join(scope)
    }
    webbrowser.open(f'{endpoint}?{requests.compat.urlencode(params)}')

def fetch_auth_code():
    """
    Continuously fetches the authorization code from the local server.
    """
    url = "http://localhost:3000/auth_code"
    time.sleep(5) # Wait for the server to start
    while True:
        print("Fetching auth code...")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if "auth_code" in data:
                print("Auth code received:", data["auth_code"])
                return data["auth_code"]
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        time.sleep(1)

def exchange_auth_code(code):
    """
    Exchange the authorization code for an access token.
    """
    url = 'https://accounts.spotify.com/api/token'
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode(),
    }

    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
        tokens = response.json()

        os.makedirs("temp", exist_ok=True)

        if 'access_token' in tokens:
            with open(ACCESS_TOKEN_PATH, "w") as f:
                f.write(tokens['access_token'])

        if 'refresh_token' in tokens:
            with open(REFRESH_TOKEN_PATH, "w") as f:
                f.write(tokens['refresh_token'])

        return tokens

    except requests.exceptions.RequestException as e:
        print(f"Error exchanging auth code: {e}")
        return {}

def get_user_token():
    """
    Retrieves the access token, refreshing it if necessary.
    """
    if os.path.exists(ACCESS_TOKEN_PATH):
        token_age = time.time() - os.path.getmtime(ACCESS_TOKEN_PATH)
        if token_age < 3540:
            with open(ACCESS_TOKEN_PATH, "r") as f:
                return f.readline().strip()

    if not os.path.exists(REFRESH_TOKEN_PATH):
        raise FileNotFoundError("Refresh token not found")

    with open(REFRESH_TOKEN_PATH, "r") as f:
        refresh_token = f.read()

    url = 'https://accounts.spotify.com/api/token'
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode(),
    }

    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
        tokens = response.json()

        if 'access_token' in tokens:
            with open(ACCESS_TOKEN_PATH, "w") as f:
                f.write(tokens['access_token'])
            return tokens['access_token']

    except requests.exceptions.RequestException as e:
        print(f"Error refreshing token: {e}")
        return None

def login(scope=None):
    """
    Initiates the login process.
    """
    user_auth(scope)
    print("Waiting for authorization...")
    auth_code = fetch_auth_code()
    exchange_auth_code(auth_code)
    print("Authorization successful.")

if __name__ == "__main__":
    if not get_user_token(): login()
    else: print("Already logged in.")

    print("Access token:", get_user_token())

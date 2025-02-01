from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import os

hostName = "localhost"
serverPort = 3000

class SpotifyAuthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            # Parse the query string
            query = urllib.parse.urlparse(self.path).query
            code_list = urllib.parse.parse_qs(query).get("code", None) # Returns a list containting one element 
            if code_list: 
                code = code_list[0]
                # Log the authorization code to the console for debugging purposes
                print(f"Authorization code: {code}")

                # Pass token to the main program
                os.makedirs("temp", exist_ok=True)
                with open("temp/auth_token", "w") as token_file:
                    token_file.write(code)
        
        # Send response 
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        html_content = f"""
        <html>
            <head><title>User Authorization</title></head>
            <body>
                <h1>Authorization Successful</h1>
                <p>You can close this window now.</p>
            </body>
        </html>
        """
        self.end_headers()
        self.wfile.write(html_content.encode())
        # /callback?code=AQ...TXWAg

if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), SpotifyAuthServer)
    print(f"Server started http://{hostName}:{serverPort}")

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        webServer.server_close()
        print("Server stopped.")
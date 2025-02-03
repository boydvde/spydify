from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import json

# TODO: Not hardcode the host and port
hostName = "localhost"
serverPort = 3000
auth_code = None

class SpotifyAuthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        if self.path.startswith("/callback"):
            # Parse the query string
            query = urllib.parse.urlparse(self.path).query
            codes = urllib.parse.parse_qs(query).get("code", None) # Returns a list containing one element 
            
            if codes and codes[0]:
                auth_code = codes[0] # Save the authorization code to memory
                
                # Send response 
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                html_content = """
                <html>
                    <head><title>User Authorization</title></head>
                    <body>
                        <h1>Authorization Successful</h1>
                        <p>You can close this window now.</p>
                    </body>
                </html>
                """
                self.wfile.write(html_content.encode())
        
        elif self.path == "/auth_code":
            print(auth_code)
            # Serve the auth_code
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"auth_code": auth_code}).encode())

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
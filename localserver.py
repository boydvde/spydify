from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import os

hostName = "localhost"
serverPort = 3000

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            # Parse the query string
            query = urllib.parse.urlparse(self.path).query
            code = urllib.parse.parse_qs(query).get("code", None)

            if code and code[0]:
                # Log the authorization code to the console for debugging purposes
                print(f"Authorization code: {code[0]}")

                # Pass token to the main program
                os.makedirs("temp", exist_ok=True)
                with open("temp/auth_token", "w") as token_file:
                    token_file.write(code[0])
        
        # Send response 
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        html_content = f"""
        <html>
            <head><title>User Authorization</title></head>
            <body>
                <p>Request: {self.path}</p>
                <p>You can close this window now.</p>
            </body>
        </html>
        """
        self.end_headers()
        self.wfile.write(html_content.encode())
        # /callback?code=AQ...TXWAg

if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print(f"Server started http://{hostName}:{serverPort}")

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        webServer.server_close()
        print("Server stopped.")
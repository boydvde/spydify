from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse

hostName = "localhost"
serverPort = 3000

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            # Parse the query string
            query = urllib.parse.urlparse(self.path).query
            code = urllib.parse.parse_qs(query).get("code", None)

            # Log code to console
            print(f"Authorization code: {code[0]}")

            # Pass token to the main program
            if code:
                with open("temp/auth_token", "w") as token_file:
                    token_file.write(code[0])
        
        # Send response 
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(f'''
                         <html>
                            <head><title>User Authorization</title></head>
                            <body>
                                <p>Request: {self.path}</p>
                                <p>You can close this window now.</p>
                            </body>
                         </html>
                         '''.encode())
        # /callback?code=AQ...TXWAg

if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
"""Serve the isolated OBS Browser Sources on the loopback interface."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

HERE = Path(__file__).resolve().parent
ASSETS = {
    "/": (HERE / "assets/stage.html", "text/html; charset=utf-8"),
    "/architecture.svg": (HERE / "architecture.svg", "image/svg+xml"),
}


class Stage:
    def __init__(self):
        self.views = {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                route = self.path.split("?", 1)[0]
                if route.startswith("/view/"):
                    content = json.dumps(self.server.views.get(route.removeprefix("/view/"), {})).encode()
                    mime = "application/json"
                elif route in ASSETS:
                    file, mime = ASSETS[route]
                    content = file.read_bytes()
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(content)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.views = self.views
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def show(self, index, **data):
        self.views[str(index)] = data

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

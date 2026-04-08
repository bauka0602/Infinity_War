import logging
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from app.config import DB_ENGINE, DB_FILE, HOST, PORT
from app.db import ensure_database
from app.http_handler import ApiHandler


def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ensure_database()
    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend started at http://{HOST}:{PORT}")
    if DB_ENGINE == "postgres":
        print("Database engine: PostgreSQL")
    else:
        print(f"SQLite database: {DB_FILE}")
    server.serve_forever()


if __name__ == "__main__":
    run()

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
_db_override = os.environ.get("ALGO_SPHERE_DB_PATH")
if _db_override:
    DB_PATH = Path(_db_override).expanduser().resolve()
    DATA_DIR = DB_PATH.parent
else:
    DB_PATH = DATA_DIR / "bots.db"

DEFAULT_API_HOST = os.environ.get("ALGO_SPHERE_API_HOST", "127.0.0.1")
DEFAULT_API_PORT = int(os.environ.get("ALGO_SPHERE_API_PORT", "8000"))
_API_URL_OVERRIDE = os.environ.get("ALGO_SPHERE_API_URL", "").strip().rstrip("/")
DEFAULT_API_URL = _API_URL_OVERRIDE or f"http://{DEFAULT_API_HOST}:{DEFAULT_API_PORT}"

"""Config from environment."""
import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent.parent
# mobius-config may be sibling (Mobius/mobius-config) when run from Mobius
_config_dir = _repo_root.parent / "mobius-config"
if not _config_dir.exists():
    _config_dir = _repo_root.parent.parent / "mobius-config"
if _config_dir.exists() and str(_config_dir) not in sys.path:
    sys.path.insert(0, str(_config_dir))
try:
    from env_helper import load_env
    load_env(_repo_root)
except ImportError:
    from dotenv import load_dotenv
    load_dotenv(_repo_root / ".env", override=True)

# GCS
GCS_BUCKET = os.getenv("GCS_BUCKET", "mobius-rag-uploads-mobiusos")

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SCRAPER_REQUEST_KEY = os.getenv("SCRAPER_REQUEST_KEY", "mobius:scraper:requests")
SCRAPER_RESPONSE_KEY_PREFIX = os.getenv("SCRAPER_RESPONSE_KEY_PREFIX", "mobius:scraper:response:")
SCRAPER_RESPONSE_TTL_SECONDS = int(os.getenv("SCRAPER_RESPONSE_TTL_SECONDS", "86400"))
SCRAPER_STREAM_CHANNEL_PREFIX = os.getenv("SCRAPER_STREAM_CHANNEL_PREFIX", "mobius:scraper:stream:")

# Tree scan
TREE_MAX_DEPTH = int(os.getenv("TREE_MAX_DEPTH", "3"))
TREE_MAX_PAGES = int(os.getenv("TREE_MAX_PAGES", "50"))
SCOPE_MODE = os.getenv("SCOPE_MODE", "same_domain")

# Document types (comma-separated)
_DOC_TYPES = os.getenv("DOCUMENT_TYPES", "pdf,doc,docx,xls,xlsx,ppt,pptx")
DOCUMENT_TYPES = [x.strip().lower() for x in _DOC_TYPES.split(",") if x.strip()]

# Summary (optional: OPENAI_API_KEY or VERTEX_PROJECT_ID)
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
VERTEX_PROJECT_ID = (os.getenv("VERTEX_PROJECT_ID") or "").strip()

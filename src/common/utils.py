import os
import zipfile
import io
import logging
from dotenv import load_dotenv

load_dotenv()
# Try to make Requests trust Windows certificate store if available (helps behind corp proxies)
try:
    import certifi_win32  # noqa: F401
except Exception:
    pass

logging.basicConfig(
    level=os.getenv("LOG_LEVEL","INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("inventory-pipeline")

DATA_DIR = os.getenv("DATA_DIR","./data")
os.makedirs(DATA_DIR, exist_ok=True)

def save_zip_and_extract(xbytes, extract_to, wanted_ext=(".xlsx",".xls",".csv")) -> str:
    """
    Accept raw ZIP bytes, extract into `extract_to`, and return the path to the
    first file matching `wanted_ext` (preferring the ZIP order).
    """
    with zipfile.ZipFile(io.BytesIO(xbytes)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(wanted_ext) and not n.endswith("/")]
        if not names:
            raise RuntimeError("No CSV/XLSX file found in ZIP.")
        target = names[0]
        z.extract(target, extract_to)
        out_path = os.path.abspath(os.path.join(extract_to, target))
        return out_path

import os
import base64
import requests
import msal
from .utils import logger, DATA_DIR, save_zip_and_extract


def _ensure_valid_ca_bundle():
    """If REQUESTS_CA_BUNDLE/SSL_CERT_FILE points to a missing file, unset it to avoid TLS errors."""
    for key in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        val = os.getenv(key)
        if val and not os.path.exists(val):
            try:
                os.environ.pop(key, None)
                logger.warning(f"Unset {key} because file not found: {val}")
            except Exception:
                pass

_ensure_valid_ca_bundle()

TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_USER    = os.getenv("GRAPH_USER")  # e.g., proshop@tennisclubs.ca
MGG_SUBJECT   = os.getenv("MGG_SUBJECT", "Mint Green Group - Daily Inventory Availability")

AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE      = ["https://graph.microsoft.com/.default"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

def _token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL auth failed: {result}")
    return result["access_token"]

def _find_latest_message_id(token: str, subject_contains: str) -> str:
    """Return newest message ID whose subject contains the substring (case-insensitive)."""
    headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
    url = f"{GRAPH_BASE}/users/{GRAPH_USER}/messages?$orderby=receivedDateTime desc&$top=20"
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    msgs = r.json().get("value", [])
    if not msgs:
        raise RuntimeError("No messages returned from mailbox.")
    needle = subject_contains.lower()
    for m in msgs:
        if needle in (m.get("subject","").lower()):
            return m["id"]
    raise RuntimeError(f"No messages found with subject containing: {subject_contains}")

def fetch_latest_mgg_zip_bytes(subject_override: str = None) -> bytes:
    """
    Return raw bytes for the first .zip attachment on the newest matching email.
    Default subject taken from MGG_SUBJECT if subject_override is None.
    """
    token = _token()
    subject = subject_override or MGG_SUBJECT
    msg_id = _find_latest_message_id(token, subject)

    att_url = f"{GRAPH_BASE}/users/{GRAPH_USER}/messages/{msg_id}/attachments?$top=20"
    ar = requests.get(att_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    ar.raise_for_status()

    for att in ar.json().get("value", []):
        if att.get("@odata.type") == "#microsoft.graph.fileAttachment" and att["name"].lower().endswith(".zip"):
            full = requests.get(
                f"{GRAPH_BASE}/users/{GRAPH_USER}/messages/{msg_id}/attachments/{att['id']}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30
            )
            full.raise_for_status()
            content_b64 = full.json()["contentBytes"]
            return base64.b64decode(content_b64)

    raise RuntimeError("No .zip attachment found on the latest matching email.")

def fetch_latest_zip_file_path(subject_override: str = None) -> str:
    """
    Convenience: download latest matching ZIP and return the extracted CSV/XLSX file path.
    """
    zbytes = fetch_latest_mgg_zip_bytes(subject_override=subject_override)
    x_path = save_zip_and_extract(zbytes, DATA_DIR, wanted_ext=(".csv",".xlsx",".xls"))
    logger.info(f"Extracted file → {x_path}")
    return x_path

if __name__ == "__main__":
    logger.info("Fetching MGG ZIP from Outlook…")
    # 1) Download the ZIP as bytes
    zbytes = fetch_latest_mgg_zip_bytes()
    # 2) Extract first CSV/XLSX inside ZIP into ./data and get its path
    file_path = save_zip_and_extract(zbytes, DATA_DIR, wanted_ext=(".csv",".xlsx",".xls"))
    logger.info(f"Extracted file → {file_path}")
    with open(os.path.join(DATA_DIR, "run_marker.txt"), "w") as f:
        f.write("Script completed successfully.\n")

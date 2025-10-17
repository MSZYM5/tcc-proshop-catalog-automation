import os
import time
import requests
import pandas as pd

# Ensure .env and logging are initialized by importing common utils.
try:
    # Relative import when used within the package
    from .utils import logger  # noqa: F401 (import for side effects: load_dotenv + logging)
except Exception:
    # Fallback if run as a standalone module
    from dotenv import load_dotenv
    load_dotenv()
    import logging
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("shopify-rest")


def _require_env() -> tuple[str, str]:
    store = os.getenv("SHOPIFY_STORE_DOMAIN")
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not store or not token:
        raise RuntimeError(
            "Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN in environment (.env)."
        )
    return store, token


def _base_and_headers() -> tuple[str, dict]:
    store, token = _require_env()
    base = f"https://{store}/admin/api/2025-01"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    return base, headers


def _requests_verify():
    """Return verify setting for requests: path, True/False based on env.

    Priority:
    - If SHOPIFY_VERIFY_SSL is false/0/no => return False (insecure, for diagnostics)
    - If SHOPIFY_CA_BUNDLE/REQUESTS_CA_BUNDLE/SSL_CERT_FILE points to a file => return that path
    - Else return True (default certifi bundle)
    """
    v = os.getenv("SHOPIFY_VERIFY_SSL")
    if v and str(v).strip().lower() in {"0", "false", "no"}:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        return False
    for key in ("SHOPIFY_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        p = os.getenv(key)
        if p and os.path.exists(p):
            return p
    return True


def _get_with_retry(url: str, headers: dict, *, timeout: int = 60, max_retries: int = 3, verify=None):
    """GET with basic retry on 429 and selected transient errors."""
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, verify=verify)
            # Retry on 429 (rate limit); surface other HTTP errors directly
            if resp.status_code == 429 and attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            # Simple backoff for transient network errors
            time.sleep(backoff)
            backoff *= 2


def test_products_connectivity(limit: int = 1) -> dict:
    """
    Minimal connectivity test against Shopify Admin REST API.
    Performs GET /products.json?limit=1 and returns parsed JSON.

    Raises with a clear message on common misconfigurations (401/403/404).
    """
    base, headers = _base_and_headers()
    url = f"{base}/products.json?limit={int(limit)}"
    verify = _requests_verify()
    try:
        r = _get_with_retry(url, headers=headers, timeout=30, verify=verify)
        return r.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        detail = None
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text if getattr(e, "response", None) is not None else str(e)
        if status == 401:
            raise RuntimeError("Unauthorized (401): Check Admin API access token.") from e
        if status == 403:
            raise RuntimeError(
                "Forbidden (403): Token lacks required Admin API scopes or is Storefront token."
            ) from e
        if status == 404:
            raise RuntimeError(
                "Not Found (404): Verify SHOPIFY_STORE_DOMAIN and API version path."
            ) from e
        raise RuntimeError(f"HTTP error {status}: {detail}") from e


def fetch_products_snapshot() -> pd.DataFrame:
    """
    Returns DataFrame with columns:
    product_id, handle, title, tags, vendor,
    variant_id, sku, barcode, variant_title,
    option1_name, option1_value, option2_name, option2_value
    """
    base, headers = _base_and_headers()
    verify = _requests_verify()
    url = (
        f"{base}/products.json?fields="
        "id,handle,title,tags,vendor,options,variants&limit=250"
    )
    rows = []
    while True:
        r = _get_with_retry(url, headers=headers, timeout=60, verify=verify)
        products = r.json().get("products", [])
        for p in products:
            opt_names = [o.get("name") for o in p.get("options",[])]
            for v in p.get("variants", []):
                o1_name = opt_names[0] if len(opt_names)>0 else None
                o2_name = opt_names[1] if len(opt_names)>1 else None
                rows.append({
                    "product_id": p["id"],
                    "handle":     p.get("handle",""),
                    "title":      p.get("title",""),
                    "tags":       p.get("tags",""),
                    "vendor":     p.get("vendor",""),
                    "variant_id": v.get("id"),
                    "sku":        (v.get("sku") or "").strip(),
                    "barcode":    (v.get("barcode") or "").strip(),
                    "variant_title": v.get("title",""),
                    "option1_name":  o1_name,
                    "option1_value": v.get("option1"),
                    "option2_name":  o2_name,
                    "option2_value": v.get("option2"),
                })
        link = r.headers.get("Link","")
        if 'rel="next"' in link:
            import re
            m = re.search(r'<([^>]+)>;\s*rel="next"', link)
            if not m: break
            url = m.group(1)
            time.sleep(0.3)
        else:
            break
    return pd.DataFrame(rows)


if __name__ == "__main__":
    # Quick manual test without exposing secrets in logs
    try:
        base, _ = _base_and_headers()
        verify = _requests_verify()
        mode = (
            "disabled" if verify is False else (f"custom: {verify}" if isinstance(verify, str) else "default")
        )
        print(f"Testing Shopify: {base} | SSL verify: {mode}")
        j = test_products_connectivity(limit=1)
        count = len(j.get("products", [])) if isinstance(j, dict) else None
        print(f"Shopify connectivity OK. Returned {count} product(s).")
    except Exception as exc:
        print(f"Shopify connectivity FAILED: {exc}")

def _request_with_retry(method: str, path: str, json_body: dict | None = None, *, max_retries: int = 5) -> dict:
    base, headers = _base_and_headers()
    verify = _requests_verify()
    url = f"{base}{path}"
    backoff = 0.5
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method.upper(), url, headers=headers, json=json_body, timeout=60, verify=verify)
            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else backoff
                except Exception:
                    wait = backoff
                time.sleep(wait)
                backoff = min(backoff * 1.5, 5.0)
                continue
            resp.raise_for_status()
            # Optional pacing when near call limit
            call_lim = resp.headers.get("X-Shopify-Shop-Api-Call-Limit", "")  # e.g., "15/80"
            try:
                used, cap = [int(x) for x in call_lim.split("/")]
                if cap and used / cap > 0.85:
                    time.sleep(0.5)
            except Exception:
                pass
            return resp.json() if resp.content else {}
        except requests.RequestException:
            if attempt >= max_retries:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 5.0)


def _post(path: str, json_body: dict) -> dict:
    data = _request_with_retry("POST", path, json_body)
    # Global throttle between mutations (env adjustable)
    try:
        throttle_ms = int(os.getenv("SHOPIFY_THROTTLE_MS", "300"))
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)
    except Exception:
        pass
    return data


def _put(path: str, json_body: dict) -> dict:
    data = _request_with_retry("PUT", path, json_body)
    try:
        throttle_ms = int(os.getenv("SHOPIFY_THROTTLE_MS", "300"))
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)
    except Exception:
        pass
    return data


def get_locations() -> list[dict]:
    base, headers = _base_and_headers()
    verify = _requests_verify()
    url = f"{base}/locations.json"
    r = requests.get(url, headers=headers, timeout=60, verify=verify)
    r.raise_for_status()
    return r.json().get("locations", [])


def create_product(product_payload: dict) -> dict:
    # Expects payload for the "product" key
    return _post("/products.json", {"product": product_payload}).get("product", {})


def set_inventory_level(location_id: int, inventory_item_id: int, available: int) -> dict:
    body = {
        "location_id": int(location_id),
        "inventory_item_id": int(inventory_item_id),
        "available": int(available),
    }
    return _post("/inventory_levels/set.json", body)


def update_inventory_item_cost(inventory_item_id: int, cost: float) -> dict:
    body = {"inventory_item": {"id": int(inventory_item_id), "cost": float(cost)}}
    return _put(f"/inventory_items/{int(inventory_item_id)}.json", body).get("inventory_item", {})

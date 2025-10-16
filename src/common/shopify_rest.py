import os, time, requests, pandas as pd

STORE = os.getenv("SHOPIFY_STORE_DOMAIN")      # e.g., "tcc-pro-shop.myshopify.com"
TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
BASE  = f"https://{STORE}/admin/api/2025-01"
HEAD  = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

def fetch_products_snapshot() -> pd.DataFrame:
    """
    Returns DataFrame with columns:
    product_id, handle, title, tags, vendor,
    variant_id, sku, barcode, variant_title,
    option1_name, option1_value, option2_name, option2_value
    """
    url = f"{BASE}/products.json?fields=id,handle,title,tags,vendor,options,variants&limit=250"
    rows = []
    while True:
        r = requests.get(url, headers=HEAD, timeout=60); r.raise_for_status()
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

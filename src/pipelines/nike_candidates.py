from pathlib import Path
import os, pandas as pd, datetime as dt

from common.graph_mail import fetch_latest_zip_file_path  # your email+zip util wrapper
from common.shopify_rest import fetch_products_snapshot
from brands.nike_parser import load_nuorder_nike

DATA_DIR   = Path(os.getenv("DATA_DIR","data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR","output"))

def _year_score(season: str) -> int:
    try:
        y = int(str(season).split()[-1])
        this = dt.datetime.now().year
        if y in (this, this-1): return 10
        if y == this-2: return 5
        return 2
    except:
        return 2

def score_rows(df: pd.DataFrame) -> pd.DataFrame:
    # defaults (tunable later)
    min_total = 30
    min_per   = 3
    line_w    = {"NIKE - Tennis":15, "NIKE - Core":10, "NIKE - Golf":5}

    # 1) Stock (50)
    df["score_stock"] = (df["total_inventory"].clip(upper=min_total) / min_total) * 50

    # 2) Size spread (25) across S/M/L; partial credit if one missing
    def spread(row):
        s = int(row.get("S",0)); m = int(row.get("M",0)); l = int(row.get("L",0))
        present = sum(x >= min_per for x in [s,m,l])
        return (present/3) * 25
    df["score_spread"] = df.apply(spread, axis=1)

    # 3) Line priority (15)
    df["score_line"] = df["vendor"].map(line_w).fillna(0)

    # 4) Recency (10)
    df["score_recency"] = df["season"].apply(_year_score)

    df["score_total"] = df[["score_stock","score_spread","score_line","score_recency"]].sum(axis=1)
    return df

def _stylecode_series(df: pd.DataFrame) -> pd.Series:
    return (df["style_code"].astype(str) + "-" + df["color_code"].astype(str)).str.lower()

def mark_already_listed(cand: pd.DataFrame, shopify_df: pd.DataFrame) -> pd.Series:
    codes = _stylecode_series(cand)
    # Simple detection via tags/handle/title (we’ll refine later if you add style tags)
    tags  = shopify_df["tags"].astype(str).str.lower()
    handles = shopify_df["handle"].astype(str).str.lower()
    titles  = shopify_df["title"].astype(str).str.lower()
    mask = []
    for code in codes:
        hit = (tags.str.contains(code, na=False) |
               handles.str.contains(code, na=False) |
               titles.str.contains(code, na=False)).any()
        mask.append(hit)
    return pd.Series(mask, index=cand.index)

def run(output_path: Path):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) NuOrder: download ZIP → extract CSV/XLSX → load Nike rows
    nuorder_file = Path(fetch_latest_zip_file_path())  # uses your env MGG_SUBJECT
    nike_df = load_nuorder_nike(str(nuorder_file))

    # 2) Shopify snapshot (for “already listed” detection)
    shopify_df = fetch_products_snapshot()

    # 3) Score
    scored = score_rows(nike_df)

    # 4) Filter vs Shopify
    scored["already_on_shopify"] = mark_already_listed(scored, shopify_df)
    new_candidates = (scored[~scored["already_on_shopify"]]
                      .copy().sort_values("score_total", ascending=False))
    already = scored[scored["already_on_shopify"]].copy()

    # 5) Export Excel with two tabs
    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        new_candidates.to_excel(xw, index=False, sheet_name="New Candidates")
        already.to_excel(xw, index=False, sheet_name="Already on Shopify")

    print(f"✅ Wrote {output_path} — New: {len(new_candidates)}, Existing: {len(already)}")

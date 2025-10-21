from pathlib import Path
from typing import Optional
import os
import pandas as pd

from common.graph_mail import fetch_latest_zip_file_path
from common.shopify_rest import fetch_products_snapshot
from brands.nike_parser import load_nuorder_nike

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))


def score_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Stock-only ranking for simplicity; add a size distribution signal for display
    min_total = 30
    df["score_stock"] = (df["total_inventory"].clip(upper=min_total) / min_total) * 100
    df["score_total"] = df["score_stock"]

    def _dist_score(row):
        exclude = {
            "style_code",
            "color_code",
            "vendor",
            "type",
            "season",
            "total_inventory",
            "msrp",
            "nike_title_raw",
            "nike_color_name_raw",
            "sample_sku",
            "skus",
            "sku_count",
            "score_stock",
            "score_total",
            "already_on_shopify",
        }
        vals = [
            v
            for k, v in row.items()
            if k not in exclude and isinstance(v, (int, float)) and v > 0
        ]
        if not vals:
            return 0.0
        total = sum(vals)
        top = max(vals)
        return max(0.0, min(100.0, (1.0 - (top / total)) * 100.0))

    df["size_dist_score"] = df.apply(_dist_score, axis=1)
    df["size_distribution"] = df["size_dist_score"].apply(
        lambda s: "Balanced" if s >= 50 else ("Mixed" if s >= 25 else "Skewed")
    )
    return df


def mark_already_listed_by_sku(cand: pd.DataFrame, shopify_df: pd.DataFrame) -> pd.Series:
    shopify_skus = set(shopify_df["sku"].astype(str).str.strip().str.lower())
    shopify_skus.discard("")
    mask = []
    for _, row in cand.iterrows():
        sku_str = str(row.get("skus", ""))
        cand_skus = [s.strip().lower() for s in sku_str.split(",") if s.strip()]
        mask.append(any(s in shopify_skus for s in cand_skus))
    return pd.Series(mask, index=cand.index)


def run_candidates(output_path: Path, subject_override: Optional[str] = None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) NuOrder: download ZIP + extract CSV/XLSX + load Nike rows
    nuorder_file = Path(fetch_latest_zip_file_path(subject_override=subject_override))
    nike_df = load_nuorder_nike(str(nuorder_file))

    # Filter out golf shoes and zero-inventory rows
    nike_df = nike_df[nike_df["type"].astype(str) != "NIKE - Golf : Shoes"].copy()
    nike_df = nike_df[nike_df["total_inventory"] > 0].copy()

    # 2) Shopify snapshot (for already listed detection)
    shopify_df = fetch_products_snapshot()

    # 3) Score
    scored = score_rows(nike_df)

    # 4) Filter vs Shopify (SKU-based)
    scored["already_on_shopify"] = mark_already_listed_by_sku(scored, shopify_df)

    # Style-level existence to flag potential new colors for an existing style
    shop_text = (
        shopify_df["tags"].astype(str) + " "
        + shopify_df["handle"].astype(str) + " "
        + shopify_df["title"].astype(str)
    ).str.lower()

    def _style_exists(style: str) -> bool:
        s = str(style).lower().strip()
        if not s:
            return False
        return shop_text.str.contains(s, na=False).any()

    scored["style_exists_on_shopify"] = scored["style_code"].apply(_style_exists)
    scored["new_color"] = (~scored["already_on_shopify"]) & (scored["style_exists_on_shopify"]) 

    # Curate columns for readability in export
    columns_display = [
        "style_code",
        "color_code",
        "vendor",
        "type",
        "season",
        "nike_title_raw",
        "nike_color_name_raw",
        "total_inventory",
        "msrp",
        "size_distribution",
        "size_dist_score",
        "sku_count",
        "sample_sku",
        "skus",
        "score_total",
    ]
    columns_display = [c for c in columns_display if c in scored.columns]

    new_candidates_df = (
        scored[~scored["already_on_shopify"]]
        .copy()
        .sort_values("score_total", ascending=False)
    )
    if "new_color" in new_candidates_df.columns:
        new_candidates_df["new_color"] = new_candidates_df["new_color"].map(lambda x: "Yes" if bool(x) else "No")
    new_candidates = new_candidates_df[
        columns_display + (["new_color"] if "new_color" in new_candidates_df.columns else [])
    ]

    already = (
        scored[scored["already_on_shopify"]]
        .copy()
        .sort_values(["total_inventory", "style_code", "color_code"], ascending=[True, True, True])
    )[columns_display]

    # 5) Export Excel with two tabs
    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        new_candidates.to_excel(xw, index=False, sheet_name="New Candidates")
        already.to_excel(xw, index=False, sheet_name="Already on Shopify")

    print(f"Wrote {output_path} - New: {len(new_candidates)}, Existing: {len(already)}")


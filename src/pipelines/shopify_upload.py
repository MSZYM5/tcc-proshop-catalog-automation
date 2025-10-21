from pathlib import Path
from typing import Optional
import os
import pandas as pd

from common.utils import logger
from common.shopify_rest import (
    fetch_products_snapshot,
    get_locations,
    create_product,
    create_variant,
    set_inventory_level,
    update_inventory_item_cost,
    update_product_tags,
    publish_product_all_channels,
)


def _load_draft(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    xls = pd.ExcelFile(path)
    products = pd.read_excel(xls, sheet_name="Products")
    variants = pd.read_excel(xls, sheet_name="Variants")
    return products, variants


def _safe_str(val) -> str:
    if pd.isna(val):
        return ""
    try:
        s = str(val)
        return s if s is not None else ""
    except Exception:
        return ""


def _build_product_payload(prod_row: pd.Series, var_rows: pd.DataFrame, publish_status: str) -> dict:
    title = str(prod_row.get("Title", "")).strip()
    handle = str(prod_row.get("Handle", "")).strip().lower().replace(" ", "-")
    body_html = str(prod_row.get("Body HTML", "")).strip()
    vendor = str(prod_row.get("Vendor", "Nike")).strip() or "Nike"
    product_type = str(prod_row.get("Product Type", "")).strip() or None
    tags = str(prod_row.get("Tags", "")).strip()

    # Variants: two options Color, Size
    variants_payload = []
    def _size_sort_key(s: str):
        s = _safe_str(s).strip().upper()
        # Numeric sizes (shoes)
        try:
            return (0, float(s))
        except Exception:
            pass
        order = ["2XS","XXS","XS","S","M","L","XL","2XL","3XL","4XL","5XL"]
        if s in order:
            return (1, order.index(s))
        return (2, s)

    # Compute normalized columns for sorting
    tmp = var_rows.copy()
    tmp["_size_val"] = tmp.apply(lambda r: _safe_str(r.get("Option2 Value", r.get("size",""))).strip(), axis=1)
    tmp["_color_val"] = tmp.apply(lambda r: (_safe_str(r.get("Option1 Value","")) or _safe_str(r.get("Color (by code)","")) or _safe_str(r.get("color_code",""))).strip(), axis=1)
    # Build sort keys
    tmp["_size_key"] = tmp["_size_val"].apply(_size_sort_key)
    tmp = tmp.sort_values(by=["_color_val","_size_key"], kind="mergesort")

    for _, vr in tmp.iterrows():
        size = _safe_str(vr.get("_size_val")).strip() or "Default"
        color = _safe_str(vr.get("_color_val")).strip()
        sku = _safe_str(vr.get("Variant SKU", "")).strip()
        price = vr.get("Variant Price", None)
        compare_at = vr.get("Variant Compare At Price", None)
        variant = {
            "option1": color or None,
            "option2": size or None,
            "sku": sku or None,
            "price": None if pd.isna(price) else float(price),
            "inventory_management": "shopify",
            "fulfillment_service": "manual",
        }
        if pd.notna(compare_at) and _safe_str(compare_at).strip() != "":
            variant["compare_at_price"] = float(compare_at)
        variants_payload.append(variant)

    payload = {
        "title": title,
        "body_html": body_html,
        "vendor": vendor,
        "status": publish_status,
        "tags": tags,
        "handle": handle,
        "options": [{"name": "Color"}, {"name": "Size"}],
        "variants": variants_payload,
    }
    if product_type:
        payload["product_type"] = product_type
    if publish_status == "active":
        # Fallback to ensure Online Store visibility on older setups
        payload["published_scope"] = "web"
    return payload


def _synthesize_product_row(style_code: str, var_rows: pd.DataFrame) -> pd.Series:
    """Create a minimal product row from variants when Products sheet lacks the style."""
    # Prefer Expanded Title from merged variants; fallback to nike_title_raw
    expanded = None
    if "Expanded Title" in var_rows.columns:
        expanded = var_rows["Expanded Title"].dropna().astype(str).str.strip()
        expanded = expanded.iloc[0] if not expanded.empty else None
    if not expanded and "nike_title_raw" in var_rows.columns:
        expanded = var_rows["nike_title_raw"].dropna().astype(str).str.strip()
        expanded = expanded.iloc[0] if not expanded.empty else "Product"
    title = f"Nike {expanded}".strip()
    # Tags: union across variants
    tags = []
    if "Tags" in var_rows.columns:
        uniq = set()
        for t in var_rows["Tags"].dropna().astype(str):
            for part in [p.strip() for p in t.split(",") if p.strip()]:
                uniq.add(part)
        tags = ", ".join(sorted(uniq))
    pt = None
    if "Product Type" in var_rows.columns:
        pt_series = var_rows["Product Type"].dropna().astype(str)
        pt = pt_series.iloc[0] if not pt_series.empty else None
    season = None
    if "season" in var_rows.columns:
        s = var_rows["season"].dropna().astype(str)
        season = s.iloc[0] if not s.empty else None
    data = {
        "style_code": str(style_code).upper().strip(),
        "Title": title,
        "Handle": f"nike-{str(style_code).lower().strip()}",
        "Vendor": "Nike",
        "Product Type": pt,
        "Tags": tags,
        "Body HTML": "",
        "season": season,
    }
    return pd.Series(data)


def upload_from_draft(
    draft_path: Path,
    publish_status: str = "draft",
    set_inventory: bool = True,
    set_cost: bool = True,
) -> list[dict]:
    """
    Create products + variants in Shopify from the listings draft workbook.
    Returns a list of result records per product created/skipped.
    """
    products_df, variants_df = _load_draft(draft_path)

    # Normalize keys (strip + uppercase for style_code; keep color_code as string)
    for df in (products_df, variants_df):
        if "style_code" in df.columns:
            df["style_code"] = df["style_code"].astype(str).str.upper().str.strip()
        if "color_code" in df.columns:
            df["color_code"] = df["color_code"].astype(str).str.strip()
    if "Style-Color" in variants_df.columns:
        variants_df["Style-Color"] = variants_df["Style-Color"].astype(str)

    # Existing handles and SKUs to avoid duplicates; also capture tags and option names
    try:
        snapshot = fetch_products_snapshot()
        existing_handles = set(snapshot["handle"].astype(str))
        # Build maps: handle -> product_id, product_id -> set(skus), product_id -> existing tags
        handle_to_pid = {}
        pid_to_skus = {}
        pid_to_tags = {}
        pid_to_option_names = {}
        for _, row in snapshot.iterrows():
            pid = int(row["product_id"]) if pd.notna(row["product_id"]) else None
            if not pid:
                continue
            h = str(row.get("handle",""))
            handle_to_pid[h] = pid
            sku = str(row.get("sku",""))
            if pid not in pid_to_skus:
                pid_to_skus[pid] = set()
            if sku:
                pid_to_skus[pid].add(sku)
            # Tags: same across rows; just capture first
            if pid not in pid_to_tags:
                pid_to_tags[pid] = str(row.get("tags",""))
            # Option names captured from any row
            o1 = str(row.get("option1_name",""))
            o2 = str(row.get("option2_name",""))
            pid_to_option_names[pid] = (o1, o2)
    except Exception as e:
        logger.warning(f"Upload: could not fetch existing products ({e}); proceeding without duplicate handle check")
        existing_handles = set()
        handle_to_pid = {}
        pid_to_skus = {}
        pid_to_tags = {}
        pid_to_option_names = {}

    # Gather locations once for inventory
    locations = []
    if set_inventory:
        try:
            locations = get_locations()
        except Exception as e:
            logger.warning(f"Upload: could not get locations ({e}); inventory not set")
            locations = []

    # Group by style_code to form one product per style across colors
    results = []
    prods_by_key = products_df.set_index(["style_code"], drop=False)
    available_styles = set(prods_by_key.index.astype(str))
    publish_active = (publish_status == "active")
    for grp_key, group in variants_df.groupby(["style_code"]):
        sc = grp_key[0] if isinstance(grp_key, (tuple, list)) else grp_key
        key = str(sc).strip().upper()
        # find product row by style
        if key not in available_styles:
            logger.warning(f"Upload: synthesizing product row for missing style {key}")
            prod_row = _synthesize_product_row(sc, group)
        else:
            prod_row = prods_by_key.loc[(key)] if key in prods_by_key.index else prods_by_key.loc[(sc)]
            # If multiple rows for same style, pick the first
            if isinstance(prod_row, pd.DataFrame):
                prod_row = prod_row.iloc[0]
        handle = str(prod_row.get("Handle", "")).strip()
        if handle and handle in existing_handles:
            # Update existing product: add new variants that don't exist by SKU and merge tags
            pid = handle_to_pid.get(handle)
            existing_skus = pid_to_skus.get(pid, set())
            added = 0
            # Merge tags
            prior_tags = pid_to_tags.get(pid, "")
            new_tags = str(prod_row.get("Tags",""))
            merged = ", ".join(sorted({t.strip() for t in (prior_tags + "," + new_tags).split(",") if t.strip()}))
            try:
                update_product_tags(pid, merged)
            except Exception as e:
                logger.warning(f"Upload: update tags failed for product {pid}: {e}")

            # Add new variants
            for _, vr in group.iterrows():
                sku = _safe_str(vr.get("Variant SKU",""))
                if not sku or sku in existing_skus:
                    continue
                color = _safe_str(vr.get("Option1 Value","")) or _safe_str(vr.get("Color (by code)","")) or _safe_str(vr.get("color_code",""))
                size  = _safe_str(vr.get("Option2 Value","")) or _safe_str(vr.get("size","")) or "Default"
                price = vr.get("Variant Price", None)
                compare_at = vr.get("Variant Compare At Price", None)
                v_payload = {
                    "option1": color or None,
                    "option2": size or None,
                    "sku": sku,
                    "price": None if pd.isna(price) else float(price),
                    "inventory_management": "shopify",
                    "fulfillment_service": "manual",
                }
                if pd.notna(compare_at) and _safe_str(compare_at).strip() != "":
                    v_payload["compare_at_price"] = float(compare_at)
                try:
                    v_created = create_variant(pid, v_payload)
                    inv_item_id = v_created.get("inventory_item_id")
                    # Cost
                    if set_cost:
                        cost = vr.get("Cost per item", None)
                        if inv_item_id and pd.notna(cost):
                            try:
                                update_inventory_item_cost(inv_item_id, float(cost))
                            except Exception as e:
                                logger.warning(f"Upload: cost update failed for SKU {sku}: {e}")
                    # Inventory
                    if set_inventory and locations:
                        qty = vr.get("Variant Inventory Qty", None)
                        if pd.notna(qty):
                            for loc in locations:
                                try:
                                    set_inventory_level(loc["id"], inv_item_id, int(qty))
                                except Exception as e:
                                    logger.warning(f"Upload: inventory set failed for SKU {sku} @loc {loc['id']}: {e}")
                    added += 1
                except Exception as e:
                    logger.error(f"Upload: create variant failed for {key} SKU {sku}: {e}")
            if publish_active:
                try:
                    publish_product_all_channels(pid)
                except Exception as e:
                    logger.warning(f"Upload: publish to channels failed for product {pid}: {e}")
            results.append({
                "style": key,
                "status": "updated",
                "product_id": pid,
                "handle": handle,
                "added_variants": added,
            })
            continue

        # Create new product
        payload = _build_product_payload(prod_row, group, publish_status)
        try:
            created = create_product(payload)
        except Exception as e:
            logger.error(f"Upload: create failed for {key}: {e}")
            results.append({"style": key, "status": "error", "reason": str(e)})
            continue

        created_variants = {str(v.get("sku", "")): v for v in created.get("variants", [])}
        # Inventory and cost updates
        for _, vr in group.iterrows():
            sku = str(vr.get("Variant SKU", "")).strip()
            cv = created_variants.get(sku)
            if not cv:
                continue
            inv_item_id = cv.get("inventory_item_id")
            if set_cost:
                cost = vr.get("Cost per item", None)
                try:
                    if inv_item_id and pd.notna(cost):
                        update_inventory_item_cost(inv_item_id, float(cost))
                except Exception as e:
                    logger.warning(f"Upload: cost update failed for SKU {sku}: {e}")
            if set_inventory and locations:
                qty = vr.get("Variant Inventory Qty", None)
                if pd.notna(qty):
                    for loc in locations:
                        try:
                            set_inventory_level(loc["id"], inv_item_id, int(qty))
                        except Exception as e:
                            logger.warning(f"Upload: inventory set failed for SKU {sku} @loc {loc['id']}: {e}")

        if publish_active:
            try:
                publish_product_all_channels(created.get("id"))
            except Exception as e:
                logger.warning(f"Upload: publish to channels failed for product {created.get('id')}: {e}")
        results.append({
            "style": key,
            "status": "created",
            "product_id": created.get("id"),
            "handle": created.get("handle"),
            "variant_count": len(created.get("variants", [])),
        })

    return results

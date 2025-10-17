import pandas as pd
import re

# NuOrder column names (from your sample)
COL_HANDLE = "Handle"
COL_TITLE  = "Title"
COL_VENDOR = "Vendor"
COL_TYPE   = "Type"
COL_OPT1N  = "Option1 Name"
COL_OPT1V  = "Option1 Value"   # usually Size
COL_OPT2N  = "Option2 Name"
COL_OPT2V  = "Option2 Value"   # usually Color
COL_SKU    = "Variant SKU"
COL_QTY    = "Variant Inventory Qty"
COL_MSRP   = "Variant Compare At Price"
COL_IMG    = "Image Src"
COL_STYLE  = "Other - Style Number"   # e.g., BV0217-382 (style-color)
COL_SEASON = "Other - Season"

NIKE_VENDORS = {"NIKE - Tennis","NIKE - Core","NIKE - Golf"}

_style_color_re = re.compile(r'([A-Z]{2}\d{4})-(\d{1,3})')


def _normalize_style_code(val):
    if not val:
        return None
    return str(val).strip().upper()


def _normalize_color_code(val):
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return s.zfill(3)
    m = re.match(r'^(\d{1,3})([A-Z]+)$', s, re.IGNORECASE)
    if m:
        return m.group(1).zfill(3) + m.group(2).upper()
    return s.upper()

def _split_style_color(val: str):
    if not isinstance(val, str): return (None, None)
    m = _style_color_re.search(val)
    return (m.group(1), m.group(2)) if m else (None, None)

def load_nuorder_nike(path: str) -> pd.DataFrame:
    # CSV or Excel auto-load
    if path.lower().endswith(".csv"):
        raw = pd.read_csv(path)
    else:
        raw = pd.read_excel(path)

    df = raw[raw[COL_VENDOR].isin(NIKE_VENDORS)].copy()

    # Prefer "Other - Style Number"; fallback to the end of Handle
    sc_src = df[COL_STYLE].fillna(df[COL_HANDLE])
    parsed = sc_src.apply(_split_style_color)
    df["style_code"] = [_normalize_style_code(p[0]) for p in parsed]
    df["color_code"] = [_normalize_color_code(p[1]) for p in parsed]

    # numeric cleanup
    df[COL_QTY]  = pd.to_numeric(df[COL_QTY], errors="coerce").fillna(0).astype(int)
    df[COL_MSRP] = pd.to_numeric(df[COL_MSRP], errors="coerce")

    # canonical fields
    df["size"] = df[COL_OPT1V].astype(str).str.strip()
    df["nike_color_name_raw"] = df[COL_OPT2V].astype(str).str.strip()
    df["nike_title_raw"] = df[COL_TITLE].astype(str).str.strip()

    # group per (style,color)
    base = (
        df.groupby(["style_code", "color_code", COL_VENDOR, COL_TYPE, COL_SEASON], dropna=True)
        .agg(
            total_inventory=(COL_QTY, "sum"),
            msrp=(COL_MSRP, "max"),
            sample_title=(COL_TITLE, "first"),
            sample_color=(COL_OPT2V, "first"),
            sample_sku=(COL_SKU, "first"),
            sku_count=(COL_SKU, lambda s: len(set(str(x).strip() for x in s.dropna().astype(str) if str(x).strip()))),
            sku_list=(
                COL_SKU,
                lambda s: ",".join(
                    sorted(
                        {
                            str(x).strip()
                            for x in s.dropna().astype(str)
                            if str(x).strip()
                        }
                    )
                ),
            ),
        )
        .reset_index()
    )

    # pivot size→qty (XS,S,M,L,XL columns if present)
    size_pivot = df.pivot_table(index=["style_code","color_code"],
                                columns="size", values=COL_QTY,
                                aggfunc="sum", fill_value=0)
    size_pivot.columns = [str(c) for c in size_pivot.columns]
    out = base.merge(size_pivot, on=["style_code","color_code"], how="left")

    # keep display fields
    out.rename(columns={
        COL_VENDOR: "vendor",
        COL_TYPE:   "type",
        COL_SEASON: "season",
    }, inplace=True)
    out["nike_title_raw"] = out.pop("sample_title")
    out["nike_color_name_raw"] = out.pop("sample_color")
    out["sample_sku"] = out.get("sample_sku")
    out["skus"] = out.get("sku_list")

    return out


def load_nuorder_nike_variants(path: str) -> pd.DataFrame:
    """
    Return variant-level rows for Nike items with canonical fields:
    style_code, color_code, vendor, type, season,
    size, sku, qty, msrp, nike_title_raw, nike_color_name_raw
    """
    if path.lower().endswith(".csv"):
        raw = pd.read_csv(path)
    else:
        raw = pd.read_excel(path)

    df = raw[raw[COL_VENDOR].isin(NIKE_VENDORS)].copy()
    sc_src = df[COL_STYLE].fillna(df[COL_HANDLE])
    parsed = sc_src.apply(_split_style_color)
    df["style_code"] = [_normalize_style_code(p[0]) for p in parsed]
    df["color_code"] = [_normalize_color_code(p[1]) for p in parsed]

    df[COL_QTY] = pd.to_numeric(df[COL_QTY], errors="coerce").fillna(0).astype(int)
    df[COL_MSRP] = pd.to_numeric(df[COL_MSRP], errors="coerce")

    out = pd.DataFrame({
        "style_code": df["style_code"],
        "color_code": df["color_code"],
        "vendor": df[COL_VENDOR].astype(str),
        "type": df[COL_TYPE].astype(str),
        "season": df[COL_SEASON].astype(str),
        "size": df[COL_OPT1V].astype(str).str.strip(),
        "sku": df[COL_SKU].astype(str).str.strip(),
        "qty": df[COL_QTY],
        "msrp": df[COL_MSRP],
        "nike_title_raw": df[COL_TITLE].astype(str).str.strip(),
        "nike_color_name_raw": df[COL_OPT2V].astype(str).str.strip(),
    })

    # Drop rows missing key identifiers
    out = out.dropna(subset=["style_code", "color_code"])
    return out


def extract_nike_color_vocab(path: str) -> pd.DataFrame:
    """
    Return a DataFrame listing unique Nike color names found, with counts and sample style codes.
    Columns: raw_color, count, sample_styles (comma-separated up to 5)
    """
    if path.lower().endswith(".csv"):
        raw = pd.read_csv(path)
    else:
        raw = pd.read_excel(path)

    df = raw[raw[COL_VENDOR].isin(NIKE_VENDORS)].copy()
    df["raw_color"] = df[COL_OPT2V].astype(str).str.strip()
    df["style_code"] = df[COL_STYLE].fillna(df[COL_HANDLE]).apply(lambda v: _split_style_color(v)[0])

    grp = (df.groupby("raw_color")
             .agg(count=("raw_color", "size"),
                  sample_styles=("style_code", lambda s: ",".join(sorted({x for x in s.dropna().astype(str) if x})[:5])))
             .reset_index())
    return grp.rename(columns={"raw_color":"raw_color"})

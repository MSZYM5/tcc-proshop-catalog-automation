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

_style_color_re = re.compile(r'([A-Z]{2}\d{4})-(\d{3})')

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
    df["style_code"] = [p[0] for p in parsed]
    df["color_code"] = [p[1] for p in parsed]

    # numeric cleanup
    df[COL_QTY]  = pd.to_numeric(df[COL_QTY], errors="coerce").fillna(0).astype(int)
    df[COL_MSRP] = pd.to_numeric(df[COL_MSRP], errors="coerce")

    # canonical fields
    df["size"] = df[COL_OPT1V].astype(str).str.strip()
    df["nike_color_name_raw"] = df[COL_OPT2V].astype(str).str.strip()
    df["nike_title_raw"] = df[COL_TITLE].astype(str).str.strip()

    # group per (style,color)
    base = (df.groupby(["style_code","color_code",COL_VENDOR,COL_TYPE,COL_SEASON], dropna=True)
              .agg(total_inventory=(COL_QTY,"sum"),
                   msrp=(COL_MSRP,"max"),
                   sample_title=(COL_TITLE, "first"),
                   sample_color=(COL_OPT2V,"first"))
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

    return out

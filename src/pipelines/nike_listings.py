from pathlib import Path
from typing import Iterable, Optional
import os
import json
import re
import pandas as pd

from common.graph_mail import fetch_latest_zip_file_path
from common.shopify_rest import fetch_products_snapshot
from brands.nike_parser import load_nuorder_nike_variants
from common.utils import logger


def _read_lines_file(path: Optional[str]) -> list[str]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _read_selection_csv(path: Optional[str]) -> list[tuple[str, str]]:
    """
    Read style/color selections from a CSV.
    Accepts either columns [style_code, color_code] or a single [style_color].
    Returns list of (style_code, color_code) pairs (upper style, 3-digit color preserved).
    """
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    df = pd.read_csv(p)
    
    def _norm_style(sc: str) -> str:
        return str(sc).strip().upper()

    def _norm_color(cc: str) -> str:
        s = str(cc).strip()
        if not s:
            return ""
        if s.isdigit():
            return s.zfill(3)
        m = re.match(r"^(\d{1,3})([A-Za-z]+)$", s)
        if m:
            return m.group(1).zfill(3) + m.group(2).upper()
        return s.upper()
    pairs: list[tuple[str, str]] = []
    if {"style_code", "color_code"}.issubset(df.columns):
        for _, row in df.iterrows():
            sc = _norm_style(row.get("style_code", ""))
            cc = _norm_color(row.get("color_code", ""))
            if sc and cc:
                pairs.append((sc, cc))
    elif "style_color" in df.columns:
        for _, row in df.iterrows():
            val = str(row.get("style_color", "")).strip()
            if "-" in val:
                sc, cc = val.split("-", 1)
                sc = _norm_style(sc)
                cc = _norm_color(cc)
                if sc and cc:
                    pairs.append((sc, cc))
    else:
        raise ValueError("Selection CSV must have columns [style_code,color_code] or [style_color]")
    return pairs


def _normalize_color(raw_color: str, color_map_path: Optional[str]) -> str:
    if not raw_color:
        return ""
    if color_map_path and Path(color_map_path).exists():
        try:
            cmap = pd.read_csv(color_map_path)
            if {"raw_color", "normalized_color"}.issubset(cmap.columns):
                row = cmap[cmap["raw_color"].str.lower() == raw_color.lower()]
                if not row.empty:
                    return str(row.iloc[0]["normalized_color"]).strip()
        except Exception:
            pass
    return raw_color


def _normalize_colors_for_style(
    style_code: str,
    raw_colors: list[str],
    *,
    color_map_csv: Optional[str] = None,
    use_ai: bool = False,
    ai_model: Optional[str] = None,
) -> tuple[list[str], str]:
    """
    Given raw color names for a single style_code, produce unique normalized names.
    - First apply CSV mapping per color.
    - For unmapped, if use_ai is True and OPENAI_API_KEY is set, call OpenAI to suggest normalized names.
    - Fallback: title-case the raw color; ensure uniqueness by appending qualifiers from raw term.
    Returns (normalized_list, notes).
    """
    # Step 1: apply CSV map
    prelim = []
    mapping_notes = []
    for rc in raw_colors:
        mapped = _normalize_color(rc, color_map_csv)
        prelim.append(mapped)
        if mapped != rc:
            mapping_notes.append(f"{rc}->{mapped}")

    # Step 2: AI suggestion for unmapped items
    final = prelim[:]
    ai_notes = ""
    try_ai = use_ai and any(a == b for a, b in zip(prelim, raw_colors)) and os.getenv("OPENAI_API_KEY")
    if try_ai:
        try:
            from openai import OpenAI

            client = OpenAI()
            model = ai_model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            # Build a structured prompt
            unique_raw = []
            for a, b in zip(prelim, raw_colors):
                if a == b and b not in unique_raw:
                    unique_raw.append(b)
            sys = (
                "You are helping normalize Nike color names into clear variant names. "
                "Given a list of raw color names for a single style, produce a JSON array of unique, "
                "human-friendly color names suitable for Shopify variant Option values. Avoid repeating the same "
                "final name; if there are multiple blues etc., choose distinct names like 'Navy', 'Light Blue', "
                "'Royal Blue', etc. Output strictly JSON array of strings."
            )
            user = json.dumps({"style_code": style_code, "raw_colors": unique_raw})
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": user},
                ],
                temperature=0.2,
            )
            text = resp.choices[0].message.content.strip()
            suggestions = json.loads(text)
            # Build a map raw->suggestion in order
            ai_map = {}
            for rc, sug in zip(unique_raw, suggestions):
                ai_map[rc] = str(sug)
            final = [ai_map.get(b, a) for a, b in zip(prelim, raw_colors)]
            ai_notes = "AI colors used"
        except Exception as e:
            ai_notes = f"AI color suggest failed: {e}"

    # Step 3: ensure uniqueness; fallback adjustments
    used = set()
    adjusted = []
    for norm, rc in zip(final, raw_colors):
        candidate = str(norm).strip() or str(rc).title()
        base = candidate
        i = 2
        while candidate.lower() in used:
            # Append a hint from raw color token to disambiguate
            hint = rc.split()[0].title() if rc else str(i)
            candidate = f"{base} {hint if hint else i}"
            i += 1
        used.add(candidate.lower())
        adjusted.append(candidate)

    notes = "; ".join([n for n in [", ".join(mapping_notes) if mapping_notes else "", ai_notes] if n])
    return adjusted, notes


def _price_from_msrp(msrp: float, item_type: str) -> float:
    # NOTE: For MVP draft, Variant Price should be the selling price (MSRP as provided)
    if msrp is None or pd.isna(msrp):
        return None
    return round(float(msrp), 2)


def _cost_from_msrp(msrp: float, item_type: str) -> float:
    if msrp is None or pd.isna(msrp):
        return None
    t = (item_type or "").lower()
    if ("shoe" in t) or ("footwear" in t):
        return round(float(msrp) * 0.55, 2)
    return round(float(msrp) * 0.50, 2)


_shoe_size_re = re.compile(r"^[MW]\s*(\d+(?:\.\d)?)$", re.IGNORECASE)


def _normalize_size(size_val: str, item_type: str) -> str:
    s = (size_val or "").strip()
    if not s:
        return s
    t = (item_type or "").lower()
    # Only strip M/W prefixes for likely footwear
    if ("shoe" in t) or ("footwear" in t):
        m = _shoe_size_re.match(s)
        if m:
            return m.group(1)
    # Apparel normalization: unify forms like XXL->2XL, XXXL->3XL, etc., and standardize wording
    canon = s.upper().replace("-"," ").replace("  ", " ").strip()
    # Common names
    mapping = {
        "XX SMALL": "2XS", "XXS": "2XS", "2XS": "2XS",
        "X SMALL": "XS", "EXTRA SMALL": "XS",
        "SMALL": "S", "MEDIUM": "M", "LARGE": "L",
        "X LARGE": "XL", "EXTRA LARGE": "XL",
        "XX LARGE": "2XL", "XXX LARGE": "3XL",
        "XXL": "2XL", "XXXL": "3XL", "XXXXL": "4XL",
        "2X": "2XL", "3X": "3XL", "4X": "4XL",
    }
    if canon in mapping:
        return mapping[canon]
    # Normalize explicit prefixes like WOMENS S / MENS L / M L -> S/L
    tokens = [t for t in re.split(r"[\s/]+", canon) if t]
    if tokens and tokens[0] in {"WOMEN", "WOMENS", "WOMEN'S", "MENS", "MEN", "MEN'S", "BOYS", "GIRLS", "YOUTH", "KIDS", "M", "W", "G", "B", "YTH", "K"}:
        tokens = tokens[1:]
    out = " ".join(tokens)
    return out


# Title abbreviation expansion rules
_abbr_start = {
    "G": "Girls",
    "B": "Boys",
    "NK": "Nike",
    "U": "Unisex",
    "W": "Women's",
    "M": "Men's",
    "AB": "Youth",
    "AD": "Kids",
    "AO": "Naomi Osaka",
}

_abbr_any = {
    "DF": "Dri-FIT",
    "DFADV": "Dri-FIT Advantage",
    "ADV": "ADV",
    "SS": "Short Sleeve",
    "LS": "Long Sleeve",
    "SL": "Sleeveless",
    "PRT": "Print",
    "VCTRY": "Victory",
    "NVLTY": "Novelty",
    "NXT": "Next",
    "HERITGE": "Heritage",
    "ADVTG": "Advantage",
    "PQ": "PQ",
    "PLTD": "Pleated",
    "FLNCY": "Flouncy",
    "PCKBL": "Pickleball",
    "NP": "Nike Pro",
    "TGT": "Tight",
    "HTHR": "Heather",
    "AB": "Aerobill",
    "ESSNTL": "Essential",
    "SWSH": "Swoosh",
    "INDY": "Indy",
    "ARBL": "Aerobill",
    "ELSTKA": "Elastika",
    "CRP": "Crop",
    "MULTI": "Multi",
    "WVN": "Woven",
    "S": "Swoosh",
    "FUT": "Futura",
    "SNBK": "Snapback",
    "TGHT": "Tight",
    "RND": "Round",
    "FLY": "Fly",
    "RFLTV": "Reflective",
    "WSH": "Wash",
    "RVR SBL": "Reversible",
    "STRTCH": "Stretch",
    "DBLE": "Double",
    "RTRO": "Retro",
    "STRP": "Stripe",
    "CB": "Club",
    "HZ": "Half Zip",
    "FZ": "Full Zip",
    "TSHRT": "T-Shirt",
    "SHRT": "Short",
    "DRSS": "Dress",
    "PRFMNC": "Performance",
    "REG": "Regular",
    "TNS": "Tennis Skirt",
    "HC": "Hard Court",
    "THRMFLX": "Therma Flex",
    "USO": "US Open",
    "LTWT": "Lightweight",
    "BKT": "Bucket",
    "PRM": "Premium",
}


def _expand_title(raw: str) -> str:
    if not raw:
        return ""
    # Split on spaces and punctuation, but keep original order
    tokens = re.split(r"[\s/]+", raw.strip())
    out = []
    for i, tok in enumerate(tokens):
        key = tok.upper()
        repl = None
        if i == 0 and key in _abbr_start:
            repl = _abbr_start[key]
        elif key in _abbr_any:
            repl = _abbr_any[key]
        else:
            repl = tok
        out.append(repl)
    title = " ".join(out)
    # Title case light normalization (don’t alter known acronym forms)
    return title.strip()


def _load_abbr_map(path: str):
    start_map = {}
    any_map = {}
    p = Path(path)
    if p.exists():
        df = pd.read_csv(p)
        for _, row in df.iterrows():
            ab = str(row.get("abbr", "")).strip().upper()
            ph = str(row.get("phrase", "")).strip()
            if not ab or not ph:
                continue
            scope = str(row.get("scope", "any")).strip().lower()
            if scope == "start":
                start_map[ab] = ph
            else:
                any_map[ab] = ph
    else:
        # fallback: use built-ins
        start_map.update(_abbr_start)
        any_map.update(_abbr_any)
    return start_map, any_map


def _expand_title_with_map(raw: str, start_map, any_map) -> str:
    if not raw:
        return ""
    tokens = re.split(r"[\s/]+", raw.strip())
    out = []
    for i, tok in enumerate(tokens):
        key = tok.upper()
        if i == 0 and key in start_map:
            out.append(start_map[key])
        elif key in any_map:
            out.append(any_map[key])
        else:
            out.append(tok)
    return " ".join(out).strip()


def _top_category(title: str, item_type: str) -> str:
    # Heuristics for MVP: infer top-level collection
    t = (title or "").lower()
    # Start token hints
    first = t.split()[0] if t.split() else ""
    footwear = any(w in (item_type or "").lower() for w in ["shoe","footwear"]) or ("shoe" in t)
    if first in {"w", "women", "women's", "womens"}:
        return "Women's Footwear" if footwear else "Women's Apparel"
    if first in {"m", "men", "men's", "mens"}:
        return "Men's Footwear" if footwear else "Men's Apparel"
    if first in {"g", "girl", "girls", "girl's", "b", "boy", "boys", "boy's", "yth", "youth", "k", "kids", "kid's"}:
        return "Kid's Footwear" if footwear else "Girl's Apparel" if first.startswith('g') else "Boy's Apparel"
    # Fallback using type
    it = (item_type or "").lower()
    if any(w in it for w in ["youth","kid"]):
        return "Kid's Footwear" if footwear else "Boy's Apparel"
    return "Men's Footwear" if footwear else "Men's Apparel"


def _detect_gender(title: str) -> str:
    t = (title or "").lower()
    if any(k in t for k in ["girl's","girls","girl"]):
        return "girls"
    if any(k in t for k in ["boy's","boys","boy"]):
        return "boys"
    if any(k in t for k in ["women's","women","womens","ladies","w "]):
        return "women"
    if any(k in t for k in ["men's","men","mens","m "]):
        return "men"
    if any(k in t for k in ["junior","youth","kid","kids","child"]):
        return "kids"
    return "unknown"


def _top_level_tag(title: str, item_type: str) -> str:
    g = _detect_gender(title)
    t = (title or "").lower()
    is_footwear = any(k in t for k in ["shoe","sneaker","footwear"]) or ("shoe" in (item_type or "").lower())
    if is_footwear:
        if g == "women":
            return "Women's Footwear"
        if g == "men":
            return "Men's Footwear"
        return "Kid's Footwear"
    if g == "women":
        return "Women's Apparel"
    if g == "men":
        return "Men's Apparel"
    if g == "girls":
        return "Girl's Apparel"
    if g == "boys":
        return "Boy's Apparel"
    return "Accessories"  # default for unisex accessories like headwear/socks


def _load_title_category_map(path: str) -> list[tuple[str, str]]:
    p = Path(path)
    rules: list[tuple[str, str]] = []
    if p.exists():
        df = pd.read_csv(p)
        if {"keyword","category"}.issubset(df.columns):
            for _, row in df.iterrows():
                kw = str(row.get("keyword",""))
                cat = str(row.get("category",""))
                if kw and cat:
                    rules.append((kw.lower().strip(), cat.strip()))
    else:
        # Defaults based on your guidance
        defaults = [
            ("hoodie", "Jacket & Hoodies"),
            ("jacket", "Jacket & Hoodies"),
            ("fleece", "T-Shirt & Tops"),
            ("sweatshirt", "T-Shirt & Tops"),
            ("tank top", "T-Shirt & Tops"),
            ("tank", "T-Shirt & Tops"),
            ("top", "T-Shirt & Tops"),
            ("shirt", "T-Shirt & Tops"),
            ("t-shirt", "T-Shirt & Tops"),
            ("polo", "T-Shirt & Tops"),
            ("bra", "T-Shirt & Tops"),
            ("pant", "Pant"),
            ("pants", "Pant"),
            ("jogger", "Pant"),
            ("shorts", "Shorts"),
            ("skirt", "Skirts"),
            ("leggings", "Leggings"),
            ("dress", "Dress"),
            ("cap", "Headwear"),
            ("hat", "Headwear"),
            ("beanie", "Headwear"),
            ("sock", "Socks"),
            ("shoe", "Shoes"),
        ]
        rules = defaults
    return rules


def _season_sort_key(season: str) -> tuple:
    """Return a sortable key for season strings like 'Summer 2026', 'Holiday 2025'. Lower is earlier."""
    if not season or not isinstance(season, str):
        return (9999, 99)
    s = season.strip().lower()
    # Extract year as last integer in string
    year = None
    for tok in reversed(s.split()):
        if tok.isdigit():
            try:
                year = int(tok)
                break
            except Exception:
                pass
    if year is None:
        year = 9999
    # Season order: Spring < Summer < Fall < Holiday
    order = {"spring": 1, "summer": 2, "fall": 3, "autumn": 3, "holiday": 4, "winter": 5}
    rank = 99
    for name, r in order.items():
        if s.startswith(name):
            rank = r
            break
    return (year, rank)


def _map_product_type(expanded_title: str) -> Optional[str]:
    t = expanded_title.lower()
    # Detection by keywords
    if any(k in t for k in ["shoe","sneaker","footwear"]):
        return "Shoes"
    if any(k in t for k in ["cap","hat","visor","beanie","bucket"]):
        return "Headwear"
    if any(k in t for k in ["sock","socks"]):
        return "Socks"
    if any(k in t for k in ["jacket","hoodie","full zip","half zip","fz","hz"]):
        return "Jacket & Hoodies"
    if any(k in t for k in ["legging","tight"]):
        return "Leggings"
    if any(k in t for k in ["dress"]):
        return "Women's Dresses"
    if any(k in t for k in ["skirt","skort"]):
        return "Shorts & Skirts"
    if any(k in t for k in ["short "," shorts","shorts"]):
        return "Shorts"
    if any(k in t for k in ["pant","pants","trouser","jogger"]):
        return "Pant"
    if any(k in t for k in ["tee","t-shirt","top","polo","tank"]):
        return "T-Shirt & Tops"
    if any(k in t for k in ["bag","wristband","headband","sleeve","accessory","accessories"]):
        return "Accessories"
    return None


def _strip_trailing_ln(title: str) -> str:
    """Remove a trailing 'Ln' token from the end of a title (case-insensitive)."""
    return re.sub(r"[\s\-]*ln$", "", str(title).strip(), flags=re.IGNORECASE).strip()


def _load_product_type_map(path: str) -> list[tuple[str, str]]:
    """
    Load keyword->product_type mapping from CSV with columns: keyword,product_type[,priority].
    Matching is case-insensitive substring; first match in priority order wins.
    """
    p = Path(path)
    rules: list[tuple[str, str, int]] = []
    if p.exists():
        df = pd.read_csv(p)
        for _, row in df.iterrows():
            kw = str(row.get("keyword", "")).strip()
            pt = str(row.get("product_type", "")).strip()
            pr = row.get("priority", 0)
            if kw and pt:
                try:
                    pr = int(pr)
                except Exception:
                    pr = 0
                rules.append((kw.lower(), pt, pr))
        rules.sort(key=lambda x: (-x[2], x[0]))
    else:
        # Defaults based on your guidance
        defaults = [
            ("polo", "T-Shirt & Tops", 10),
            ("sleeveless", "T-Shirt & Tops", 9),
            ("long sleeve", "T-Shirt & Tops", 9),
            ("jacket", "Jacket & Hoodies", 10),
            ("hoodie", "Jacket & Hoodies", 10),
            ("sock", "Socks", 10),
            ("pant", "Pant", 10),
            ("shorts", "Shorts", 10),
            ("dress", "Women's Dresses", 10),
            ("tight", "Leggings", 10),
            ("skirt", "Shorts & Skirts", 10),
            ("skort", "Shorts & Skirts", 10),
            ("cap", "Headwear", 10),
            ("beanie", "Headwear", 10),
        ]
        rules = [(k, v, p) for (k, v, p) in defaults]
    return [(k, v) for (k, v, _) in rules]


_COLOR_CODE_MAP = {
    "451": "Navy Blue",
    "010": "Black",
    "580": "Light Purple",
    "464": "Light Blue",
    "629": "Hot Pink",
    "361": "Green",
    "100": "White",
    "657": "Red",
    "379": "Teal",
    "402": "Blue",
    "489": "Blue",
    "110": "Ivory",
    "507": "Purple",
}

def _color_from_code(code: str) -> str:
    return _COLOR_CODE_MAP.get(str(code).strip(), "")


def _load_color_code_map(path: str) -> None:
    """Load color_code -> color_name mapping from CSV (columns: color_code,color_name)."""
    p = Path(path)
    if not p.exists():
        return
    try:
        df = pd.read_csv(p)
        if {"color_code", "color_name"}.issubset(df.columns):
            for _, row in df.iterrows():
                cc = str(row.get("color_code", "")).strip()
                name = str(row.get("color_name", "")).strip()
                if not cc or not name:
                    continue
                # normalize zero-padding
                if cc.isdigit():
                    cc = cc.zfill(3)
                _COLOR_CODE_MAP[cc] = name
    except Exception:
        pass


def prepare_listings_draft(
    output_path: Path,
    *,
    select_skus: Iterable[str] = (),
    select_codes: Iterable[str] = (),
    selection_csv: Optional[str] = None,
    subject_override: Optional[str] = None,
    input_nuorder: Optional[str] = None,
    color_map_csv: Optional[str] = None,
    use_ai_colors: bool = False,
    ai_model: Optional[str] = None,
):
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    # Load latest NuOrder and filter to variants
    if input_nuorder:
        nuorder_file = Path(input_nuorder)
        if not nuorder_file.exists():
            raise RuntimeError(f"Input NuOrder file not found: {nuorder_file}")
    else:
        nuorder_file = Path(fetch_latest_zip_file_path(subject_override=subject_override))
    logger.info(f"Listings: loading NuOrder from {nuorder_file}")
    vdf = load_nuorder_nike_variants(str(nuorder_file))
    logger.info(f"Listings: loaded {len(vdf)} variant rows from NuOrder")

    # Selection filtering
    select_skus = {s.strip().lower() for s in select_skus if s and s.strip()}
    select_codes = {c.strip().lower() for c in select_codes if c and c.strip()}
    # Extend from selection CSV
    for sc, cc in _read_selection_csv(selection_csv):
        select_codes.add(f"{sc}-{cc}".lower())

    if select_skus:
        vdf = vdf[vdf["sku"].astype(str).str.strip().str.lower().isin(select_skus)]
    missing_codes = set()
    if select_codes:
        code_series = (vdf["style_code"].astype(str).str.upper() + "-" + vdf["color_code"].astype(str).str.upper())
        mask = code_series.str.lower().isin(select_codes)
        missing_codes = {c.upper() for c in select_codes if c not in code_series.str.lower().unique()}
        vdf = vdf[mask]
    logger.info(f"Listings: after selection filters -> {len(vdf)} variants")
    if missing_codes:
        logger.warning(f"Listings: selection codes not found in NuOrder data: {sorted(missing_codes)}")
        # Synthesize minimal rows for missing selections so we can proceed
        synth_rows = []
        for code in sorted(missing_codes):
            try:
                sc, cc = code.split("-", 1)
                synth_rows.append({
                    "style_code": sc.strip().upper(),
                    "color_code": cc.strip().upper(),
                    "vendor": "",
                    "type": "",
                    "season": "",
                    "size": "",
                    "sku": "",
                    "qty": 0,
                    "msrp": pd.NA,
                    "nike_title_raw": sc.strip().upper(),
                    "nike_color_name_raw": "",
                })
            except Exception:
                continue
        if synth_rows:
            vdf = pd.concat([vdf, pd.DataFrame(synth_rows)], ignore_index=True)

    # Build a temporary product-level table at style+color (for reference), then collapse to style-level
    vdf["style_color"] = vdf["style_code"].astype(str) + "-" + vdf["color_code"].astype(str)
    prod_sc = (
        vdf.groupby(["style_code", "color_code", "vendor", "type", "season", "nike_title_raw"], dropna=False)
          .agg(msrp=("msrp", "max"), total_inventory=("qty", "sum"))
          .reset_index()
    )
    logger.info(f"Listings: product-level (style+color) groups -> {len(prod_sc)} rows")

    # Load abbreviation map and expand titles on the style+color table, then collapse to style-level
    abbr_csv = os.getenv("ABBR_MAP_CSV", "config/abbr_map.csv")
    abbr_start, abbr_any = _load_abbr_map(abbr_csv)
    prod_sc["Expanded Title"] = prod_sc["nike_title_raw"].apply(lambda x: _expand_title_with_map(x, abbr_start, abbr_any))
    # Build base titles (Nike + expanded), strip trailing 'Ln', and dedupe 'Nk' if 'Nike' already present
    def _base_title(row):
        t = f"Nike {row['Expanded Title']}".strip()
        t = _strip_trailing_ln(t)
        if "Nike" in t:
            t = re.sub(r"\bNk\b", "", t, flags=re.IGNORECASE)
            t = re.sub(r"Nike\s+Nike", "Nike", t, flags=re.IGNORECASE)
            t = re.sub(r"\s{2,}", " ", t).strip()
        return t
    prod_sc["Title"] = prod_sc.apply(_base_title, axis=1)

    # Canonicalize title per style_code: choose one base title per style (first encountered season),
    # and if that base title already exists on Shopify, append the chosen season once for all colors of that style.
    try:
        sf = fetch_products_snapshot()
        existing_titles = set(sf["title"].astype(str))
    except Exception as e:
        logger.warning(f"Listings: Shopify title check skipped ({e})")
        existing_titles = set()

    style_to_title = {}
    for scode, g in prod_sc.groupby("style_code", sort=False):
        if g.empty:
            continue
        # Choose the first group's base title and its season (any one is fine as per your instruction)
        first_idx = g.index[0]
        base_title = prod_sc.loc[first_idx, "Title"]
        season = str(prod_sc.loc[first_idx, "season"]).strip()
        if base_title in existing_titles and season:
            final_title = f"{base_title} - {season}"
        else:
            final_title = base_title
        style_to_title[scode] = final_title
    # Collapse to style-level products by picking first row per style_code and applying canonical title
    prod = (prod_sc.sort_values(["style_code"]).groupby("style_code", as_index=False).first())
    if style_to_title:
        prod["Title"] = prod.apply(lambda r: style_to_title.get(r["style_code"], r["Title"]), axis=1)
    prod["Handle"] = prod.apply(
        lambda r: f"nike-{str(r['style_code']).lower()}", axis=1
    )
    prod["Vendor"] = "Nike"
    # Map product type: CSV keywords > heuristics > NuOrder type
    pt_map_csv = os.getenv("PRODUCT_TYPE_MAP_CSV", "config/product_type_map.csv")
    kw_rules = _load_product_type_map(pt_map_csv)
    def _resolve_pt(row):
        t = str(row["Expanded Title"]).lower()
        for kw, pt in kw_rules:
            if kw in t:
                return pt
        return _map_product_type(row["Expanded Title"]) or str(row.get("type","")) or None
    prod["Product Type"] = prod.apply(_resolve_pt, axis=1)
    # Tags based on title mapping + gender/top-level
    title_rules = _load_title_category_map(os.getenv("TITLE_CATEGORY_MAP_CSV", "config/title_category_map.csv"))
    def _category_from_title(title: str, gender: str) -> str | None:
        tl = title.lower()
        for kw, cat in title_rules:
            if kw in tl:
                # Special case: women's shorts -> Shorts & Skirts
                if cat == "Shorts" and gender == "women":
                    return "Shorts & Skirts"
                return cat
        return None

    def _product_tags(row):
        tags = ["Nike", str(row["style_code"]).upper()]
        title = str(row.get("Expanded Title", ""))
        gender = _detect_gender(title)
        top_tag = _top_level_tag(title, row.get("type",""))
        cat_tag = _category_from_title(title, gender)
        # Footwear: only top-level tag
        if top_tag and "Footwear" in top_tag:
            tags.append(top_tag)
        else:
            if cat_tag:
                tags.append(cat_tag)
            else:
                tags.append("Needs Category")
            if top_tag:
                tags.append(top_tag)
        season = str(row.get("season", "")).strip()
        if season:
            tags.append(season)
        return ", ".join([t for t in tags if t])
    prod["Tags"] = prod.apply(_product_tags, axis=1)
    # Collections column: Vendor and top-level category
    # Keep Collections empty (smart collections use tags)
    prod["Collections"] = ""
    prod["Body HTML"] = "<p></p>"  # leave blank for MVP
    prod["Title Notes"] = ""  # Placeholder for unknown abbreviations, etc.
    # Append season if duplicates within batch or already exist on Shopify
    try:
        sf = fetch_products_snapshot()
        existing_titles = set(sf["title"].astype(str))
    except Exception as e:
        logger.warning(f"Listings: Shopify title check skipped ({e})")
        existing_titles = set()
    dup_within = prod.duplicated(subset=["Title"], keep=False)
    dup_external = prod["Title"].isin(existing_titles)
    dup_any = dup_within | dup_external
    prod.loc[dup_any, "Title"] = prod.loc[dup_any].apply(
        lambda r: f"{r['Title']} - {r['season']}" if str(r['season']).strip() else r['Title'], axis=1
    )

    # Variant table
    variants = vdf.copy()
    variants["Style-Color"] = variants["style_code"].astype(str) + "-" + variants["color_code"].astype(str)
    variants["Option1 Name"] = "Color"
    _load_color_code_map(os.getenv("COLOR_CODE_MAP_CSV", "config/color_code_map.csv"))
    variants["Option1 Value"] = variants.apply(lambda r: (_color_from_code(r["color_code"]) or str(r["nike_color_name_raw"]).strip()), axis=1)
    variants["Option2 Name"] = "Size"
    variants["Option2 Value"] = variants.apply(lambda r: _normalize_size(str(r["size"]), str(r["type"])), axis=1)
    variants["Variant SKU"] = variants["sku"].astype(str)
    variants["Variant Inventory Qty"] = variants["qty"].astype(int)
    # MVP pricing: Variant Price = MSRP; Cost per item = 50% apparel, 55% shoes
    variants["Variant Price"] = variants["msrp"].astype(float)
    variants["Cost per item"] = variants.apply(lambda r: _cost_from_msrp(r["msrp"], r["type"]), axis=1)
    variants["Variant Compare At Price"] = ""
    variants["Variant Notes"] = variants.apply(
        lambda r: "Missing MSRP" if pd.isna(r["msrp"]) else "",
        axis=1,
    )
    # Reference color from color code for visibility
    variants["Color (by code)"] = variants["color_code"].apply(_color_from_code)

    # Construct tag set per variant (includes Special Order, style code, vendor, top category, product type)
    # We’ll reuse product-level categories by merging
    prod_light = prod[["style_code","Expanded Title","Product Type","season"]].copy()
    variants = variants.merge(prod_light, on=["style_code"], how="left")
    def _variant_tags(row):
        tags = ["Nike", str(row['style_code']).upper()]
        title = str(row.get("Expanded Title", ""))
        gender = _detect_gender(title)
        top = _top_level_tag(title, row.get("type",""))
        cat = _category_from_title(title, gender)
        if top and "Footwear" in top:
            # Footwear: only top-level tag
            tags.append(top)
        else:
            if cat:
                tags.append(cat)
            else:
                tags.append("Needs Category")
            if top:
                tags.append(top)
            # Unisex: Headwear and Socks should include Accessories
            if cat in {"Headwear","Socks"} and "Accessories" not in tags:
                tags.append("Accessories")
        season = str(row.get("season", "")).strip()
        if season:
            tags.append(season)
        return ", ".join(tags)
    variants["Tags"] = variants.apply(_variant_tags, axis=1)
    logger.info("Listings: assembled variant tags and pricing")

    # Export
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as xw:
        prod_cols = [
            "style_code","Title","Handle","Vendor","Product Type","Tags","Collections","Body HTML",
            "season","msrp","total_inventory","Title Notes",
        ]
        # Ensure Body HTML starts with Style code
        prod_out = prod.copy()
        prod_out["Body HTML"] = prod_out.apply(lambda r: f"Style: {str(r['style_code']).upper()}", axis=1)
        prod_out[[c for c in prod_cols if c in prod_out.columns]].to_excel(xw, index=False, sheet_name="Products")

        var_cols = [
            # MVP variant sheet with explicit columns requested
            "Style-Color",
            "Variant Title",  # leave blank for manual if desired
            "Variant SKU",
            "style_code",
            "color_code",
            "Color (by code)",
            "Option2 Value",  # Size (normalized)
            "Option1 Value",  # Variant color name (left blank)
            "Variant Price",
            "Cost per item",
            "Tags",
            # Supporting fields for traceability
            "Option1 Name","Option2 Name",
            "Variant Inventory Qty","Variant Compare At Price",
            "size","msrp","qty","type","Variant Notes",
        ]
        # Order variants by style_code, then color, then size order
        def _size_rank(val: str):
            s = str(val).strip().upper()
            try:
                return (0, float(s))
            except Exception:
                order = {"2XS":0,"XXS":1,"XS":2,"S":3,"M":4,"L":5,"XL":6,"2XL":7,"3XL":8,"4XL":9,"5XL":10}
                return (1, order.get(s, 999), s)
        v_out = variants.copy()
        v_out["_size_key"] = v_out["Option2 Value"].apply(_size_rank)
        v_out = v_out.sort_values(by=["style_code","Option1 Value","_size_key"], kind="mergesort")
        v_out = v_out.drop(columns=["_size_key"]) 
        v_out[[c for c in var_cols if c in v_out.columns]].to_excel(xw, index=False, sheet_name="Variants")
    logger.info(f"Listings: wrote Excel draft -> {output_path}")

    return output_path

# TCC Pro Shop – Catalog Automation

Python automation to find candidates, prepare listings, and upload products to Shopify (Admin REST API). Focused on Nike (Dunlop later).

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt`
- `.env` configured:
  - Microsoft Graph (optional if using local input):
    - `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `GRAPH_USER`, `MGG_SUBJECT`
  - Shopify Admin API:
    - `SHOPIFY_STORE_DOMAIN`, `SHOPIFY_ACCESS_TOKEN`
  - Other:
    - `DATA_DIR=./data`, `OUTPUT_DIR=./output`, `LOG_LEVEL=INFO`
    - Optional TLS: set `SHOPIFY_VERIFY_SSL=false` behind corp proxy

## Helpful Config CSVs

- `config/abbr_map.csv` – Abbreviation expansion for titles (columns: `abbr,phrase,scope`) scope=`start|any`
- `config/product_type_map.csv` – Keyword→Product Type mapping (columns: `keyword,product_type,priority`)
- `config/title_category_map.csv` – Keyword→Category tag mapping from product title (columns: `keyword,category`)
- `config/color_code_map.csv` – Nike color code→name mapping (columns: `color_code,color_name`)

Edit these to tune naming, tags, and categories without code changes.

## Workflow

1) Candidates (Nike)

Find new candidates from NuOrder vs Shopify with scoring.

Command:

```
python src/run.py candidates --brand nike \
  [--subject "Mint Green Group - Daily Inventory Availability"] \
  [--output output/nike_candidates_YYYYMMDD_HHMM.xlsx]
```

Notes:
- If Outlook/Graph is not accessible, add the local input flag in later steps (listings/upload).
- If behind corp proxy: `PowerShell> $env:SHOPIFY_VERIFY_SSL='false'`

2) Title/Color Vocab (optional)

Extract Nike color names to help complete `config/color_code_map.csv`.

```
python src/run.py vocab --brand nike --type colors \
  [--input data/nuorder_latest.xlsx] \
  --output config/nike_color_vocab.csv
```

3) Prepare Listings Draft

Generates a timestamped Excel with a Products sheet (one row per style) and a Variants sheet (one row per color/size). Variants use options Color then Size. Tags and Collections derive from title mappings and gender/top‑level detection.

Selection CSV (preferred): columns `style_code,color_code` or a single `style_color` (e.g., BV0217-382).

Command:

```
python src/run.py listings --brand nike \
  --input data/nuorder_latest.xlsx \  # or omit to fetch via Outlook
  --selection-csv config/selected_codes.csv \
  [--color-map config/color_code_map.csv] \
  [--output output/nike_listings_draft_YYYYMMDD_HHMM.xlsx]
```

Output:
- Products (style-level): Title, Handle, Vendor, Product Type, Tags, Collections, Body HTML, season, msrp, total_inventory
- Variants: Style-Color, Variant SKU, Color (Option1), Size (Option2), Price, Cost per item, Inventory Qty, Tags

Key behavior:
- One product per style (all colors/sizes are variants)
- Title expands abbreviations, strips trailing `Ln`, dedupes `Nk` when “Nike” present
- Collections column: `Nike; <Top-Level Tag>` (Headwear uses Accessories)
- Tags include: `Nike`, `<STYLE_CODE>`, `Special Order`, category from `title_category_map.csv` (or `Needs Category`), top-level (Men’s/Women’s/Girl’s/Boy’s Apparel or Men’s/Women’s/Kid’s Footwear), and `season`
- Color values from `config/color_code_map.csv` fallback to NuOrder color name
- Size ordering: numeric ascending (shoes); apparel in XS, S, M, L, XL, 2XL, 3XL…

4) Upload to Shopify

Creates products and variants via Admin REST API.

```
python src/run.py upload \
  --input output/nike_listings_draft_YYYYMMDD_HHMM.xlsx \
  [--publish draft|active] \
  [--no-inventory] [--no-cost] \
  [--throttle-ms 600]
```

Behavior:
- One product per style; options [Color, Size]
- Skips creation if handle exists
- Sets cost per item unless `--no-cost`
- Sets same inventory qty at all locations unless `--no-inventory`
- Avoids rate limits with retry/backoff and `--throttle-ms` pacing (increase if 429s)
- Writes `output/shopify_upload_report_YYYYMMDD_HHMM.csv`

## Troubleshooting

- TLS/Proxy: set `PowerShell> $env:SHOPIFY_VERIFY_SSL='false'`
- 429 Too Many Requests: increase `--throttle-ms` (e.g., 1000), or use `--no-inventory --no-cost` for initial pass
- Missing selections: check CLI log for “selection codes not found …”; ensure `color_code` is zero‑padded (e.g., 010, 063)
- Duplicate handles: product skipped; edit handle or remove the duplicate on Shopify

## Repository Layout

```
data/        # NuOrder input files (ignored)
output/      # Generated reports and upload logs
config/      # CSV mappings (abbreviations, product type, title-category, color codes)
src/
  common/    # Shopify + Graph API, utils
  brands/    # Vendor-specific parsing (Nike)
  pipelines/ # candidates, listings draft, Shopify upload
```

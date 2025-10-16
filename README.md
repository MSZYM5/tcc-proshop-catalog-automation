# TCC ProShop – Catalog Automation

A single repo for:
1) **Candidate Finder** (Nike now; Dunlop later)
2) **Brand-specific parsing & scoring**
3) **Listing generation** (create/update products on Shopify)

## Run (manual)
```bash
python src/run.py candidates --brand nike
python src/run.py candidates --brand dunlop
python src/run.py listings  --brand nike


## Layout
/data     # auto-fetched inputs (ignored)
/output   # generated reports (ignored)
/config   # thresholds, brand configs, color map, name aliases
/src
  /common     # shared utilities (API, email, normalization, reporting)
  /brands     # brand-specific parsing & rules
  /pipelines  # candidate finder, listing generator, etc.

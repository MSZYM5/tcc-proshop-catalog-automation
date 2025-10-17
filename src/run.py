import argparse
import os
from pathlib import Path
from datetime import datetime

# Ensure src/ is on path when running as a script
if __package__ is None and __name__ == "__main__":
    import sys
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

from common.utils import logger  # loads .env and logging
from pipelines.nike_candidates import run_candidates
from pipelines.nike_listings import prepare_listings_draft
from brands.nike_parser import extract_nike_color_vocab
from pipelines.shopify_upload import upload_from_draft


def _default_output_path(prefix: str = "nike_candidates") -> Path:
    out_dir = Path(os.getenv("OUTPUT_DIR", "output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return out_dir / f"{prefix}_{ts}.xlsx"


def cmd_candidates(args: argparse.Namespace) -> int:
    brand = args.brand.lower()
    if brand != "nike":
        logger.error("Only brand 'nike' is supported right now.")
        return 2

    output = Path(args.output) if args.output else _default_output_path()
    logger.info(f"Running candidates pipeline for brand={brand} -> {output}")
    subject = args.subject if args.subject else None

    # Note about SSL
    verify_env = os.getenv("SHOPIFY_VERIFY_SSL")
    if verify_env and str(verify_env).strip().lower() in {"0", "false", "no"}:
        logger.warning("Shopify SSL verification disabled via SHOPIFY_VERIFY_SSL=false (diagnostic mode)")

    run_candidates(output, subject_override=subject)
    return 0


def _split_csv_arg(val: str | None) -> list[str]:
    if not val:
        return []
    return [x.strip() for x in val.split(",") if x.strip()]


def cmd_listings(args: argparse.Namespace) -> int:
    brand = args.brand.lower()
    if brand != "nike":
        logger.error("Only brand 'nike' is supported right now.")
        return 2

    select_skus = _split_csv_arg(args.select_skus)
    select_codes = _split_csv_arg(args.select_codes)

    # Merge with files
    from pipelines.nike_listings import _read_lines_file  # reuse helper
    select_skus += _read_lines_file(args.skus_file)
    select_codes += _read_lines_file(args.codes_file)

    if not (args.selection_csv or select_skus or select_codes or args.skus_file or args.codes_file):
        logger.error("Provide --selection-csv (preferred) or one of --select-* / --*-file for selection.")
        return 2

    output = Path(args.output) if args.output else Path(os.getenv("OUTPUT_DIR", "output")) / (
        f"nike_listings_draft_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    )
    subject = args.subject if args.subject else None

    logger.info(
        f"Preparing listings draft -> {output} | selection_csv={'yes' if args.selection_csv else 'no'} | SKUs={len(select_skus)} | Codes={len(select_codes)}"
    )
    prepare_listings_draft(
        output,
        select_skus=select_skus,
        select_codes=select_codes,
        selection_csv=args.selection_csv,
        subject_override=subject,
        color_map_csv=args.color_map,
        use_ai_colors=args.use_ai_colors,
        ai_model=args.ai_model,
    )
    logger.info("Listings draft ready for review.")
    return 0


def cmd_vocab(args: argparse.Namespace) -> int:
    brand = args.brand.lower()
    if brand != "nike":
        logger.error("Only brand 'nike' is supported right now.")
        return 2
    if args.type != "colors":
        logger.error("Only vocab type 'colors' is supported right now.")
        return 2

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Pull latest NuOrder file, extract colors vocab
    if args.input:
        p = Path(args.input)
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            return 2
    else:
        from common.graph_mail import fetch_latest_zip_file_path
        p = Path(fetch_latest_zip_file_path(subject_override=args.subject))
    vocab = extract_nike_color_vocab(str(p))
    vocab.to_csv(out, index=False)
    logger.info(f"Wrote color vocab to {out}")
    return 0

def cmd_upload(args: argparse.Namespace) -> int:
    path = Path(args.input)
    if not path.exists():
        logger.error(f"Draft Excel not found: {path}")
        return 2
    publish = args.publish
    set_inventory = not args.no_inventory
    set_cost = not args.no_cost
    # Configure throttle for write calls (used in POST/PUT helpers)
    try:
        os.environ["SHOPIFY_THROTTLE_MS"] = str(int(args.throttle_ms))
    except Exception:
        pass

    # TLS diagnostic note
    verify_env = os.getenv("SHOPIFY_VERIFY_SSL")
    if verify_env and str(verify_env).strip().lower() in {"0","false","no"}:
        logger.warning("Shopify SSL verification disabled via SHOPIFY_VERIFY_SSL=false (diagnostic mode)")

    logger.info(f"Uploading to Shopify: {path} | publish={publish} | inventory={'on' if set_inventory else 'off'} | cost={'on' if set_cost else 'off'}")
    results = upload_from_draft(path, publish_status=publish, set_inventory=set_inventory, set_cost=set_cost)
    # Summarize
    created = sum(1 for r in results if r.get('status') == 'created')
    skipped = sum(1 for r in results if r.get('status') == 'skipped')
    errors  = sum(1 for r in results if r.get('status') == 'error')
    logger.info(f"Upload complete. Created={created} Skipped={skipped} Errors={errors}")
    # Optionally, write a report
    out_dir = Path(os.getenv("OUTPUT_DIR","output")); out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    report_path = out_dir / f"shopify_upload_report_{ts}.csv"
    import csv
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=sorted({k for r in results for k in r.keys()}))
        w.writeheader(); w.writerows(results)
    logger.info(f"Wrote upload report: {report_path}")
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TCC Pro Shop Automation")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_cand = sub.add_parser("candidates", help="Find and score new product candidates")
    p_cand.add_argument("--brand", default="nike", help="Brand key (currently: nike)")
    p_cand.add_argument("--subject", default=None, help="Override email subject match for NuOrder ZIP")
    p_cand.add_argument("--output", default=None, help="Output Excel path")
    p_cand.set_defaults(func=cmd_candidates)

    p_list = sub.add_parser("listings", help="Prepare product listings draft for review")
    p_list.add_argument("--brand", default="nike", help="Brand key (currently: nike)")
    p_list.add_argument("--subject", default=None, help="Override email subject for NuOrder ZIP")
    p_list.add_argument("--input", default=None, help="Optional local NuOrder CSV/XLSX path (bypass Outlook fetch)")
    p_list.add_argument("--selection-csv", default=None, help="CSV with selections: columns [style_code,color_code] or [style_color]")
    p_list.add_argument("--select-skus", default=None, help="(Optional) Comma-separated SKUs to include")
    p_list.add_argument("--select-codes", default=None, help="(Optional) Comma-separated style-color codes (e.g., BV0217-382)")
    p_list.add_argument("--skus-file", default=None, help="(Optional) Path to file containing one SKU per line")
    p_list.add_argument("--codes-file", default=None, help="(Optional) Path to file containing one style-color per line")
    p_list.add_argument("--color-map", default=None, help="CSV with columns raw_color,normalized_color for color normalization")
    p_list.add_argument("--use-ai-colors", action="store_true", help="Use OpenAI to suggest normalized color names when no map entry exists")
    p_list.add_argument("--ai-model", default=None, help="OpenAI model name (default from OPENAI_MODEL or gpt-4o-mini)")
    p_list.add_argument("--output", default=None, help="Output Excel path (default output/nike_listings_draft_YYYYMMDD_HHMM.xlsx)")
    p_list.set_defaults(func=cmd_listings)

    p_vocab = sub.add_parser("vocab", help="Extract brand vocabularies (e.g., colors) for mapping")
    p_vocab.add_argument("--brand", default="nike", help="Brand key (currently: nike)")
    p_vocab.add_argument("--type", default="colors", choices=["colors"], help="Vocab type to extract")
    p_vocab.add_argument("--subject", default=None, help="Override email subject for NuOrder ZIP")
    p_vocab.add_argument("--output", default="config/nike_color_vocab.csv", help="Output CSV path")
    p_vocab.add_argument("--input", default=None, help="Optional local CSV/XLSX path to use instead of Outlook fetch")
    p_vocab.set_defaults(func=cmd_vocab)

    p_up = sub.add_parser("upload", help="Create products + variants in Shopify from draft Excel")
    p_up.add_argument("--input", required=True, help="Path to listings draft Excel (from 'listings' command)")
    p_up.add_argument("--publish", default="draft", choices=["draft","active"], help="Product status")
    p_up.add_argument("--no-inventory", action="store_true", help="Do not set initial inventory levels")
    p_up.add_argument("--no-cost", action="store_true", help="Do not set cost per item")
    p_up.add_argument("--throttle-ms", type=int, default=300, help="Delay between write calls (ms) to avoid 429s")
    p_up.set_defaults(func=cmd_upload)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

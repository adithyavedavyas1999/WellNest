"""
Batch report generation for WellNest county equity reports.

Generates PDF reports for all counties (or a filtered subset) using the
CountyReportGenerator from the reports module.  Handles errors per county
so one bad row doesn't kill the entire batch.

Usage:
    python scripts/generate_reports.py                           # all counties
    python scripts/generate_reports.py --state IL                # only Illinois
    python scripts/generate_reports.py --fips 17031 --fips 06037 # specific counties
    python scripts/generate_reports.py --parallel 4              # 4 workers
    python scripts/generate_reports.py --format pdf --output-dir ./out
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wellnest.batch_reports")


def _get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "wellnest")
    user = os.environ.get("POSTGRES_USER", "wellnest")
    pw = os.environ.get("POSTGRES_PASSWORD", "changeme")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def fetch_county_list(
    db_url: str,
    states: list[str] | None = None,
    fips_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Pull the list of counties to generate reports for."""
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            query = """
                SELECT county_fips, county_name, state_abbr, scored_school_count
                FROM gold.county_summary
                WHERE scored_school_count > 0
            """
            params: dict[str, Any] = {}

            if fips_codes:
                query += " AND county_fips = ANY(:fips_codes)"
                params["fips_codes"] = fips_codes
            elif states:
                query += " AND state_abbr = ANY(:states)"
                params["states"] = [s.upper() for s in states]

            query += " ORDER BY state_abbr, county_name"

            rows = conn.execute(text(query), params).mappings().all()
            return [dict(r) for r in rows]
    finally:
        engine.dispose()


def generate_single_report(
    fips: str,
    county_name: str,
    state: str,
    db_url: str,
    output_dir: Path,
    report_format: str,
) -> dict[str, Any]:
    """Generate one county report.  Returns a result dict for the summary.

    This is a standalone function (not a method) so it can be pickled
    by ProcessPoolExecutor.
    """
    from reports.pdf_generator import CountyReportGenerator

    result: dict[str, Any] = {
        "fips": fips,
        "county": county_name,
        "state": state,
        "status": "ok",
        "error": None,
        "path": None,
        "elapsed_ms": 0,
    }

    start = time.monotonic()

    try:
        gen = CountyReportGenerator(db_url=db_url)
        gen.generate(fips)

        filename = f"county_{fips}_{state.lower()}.{report_format}"
        out_path = output_dir / filename
        gen.save(out_path)
        gen.close()

        result["path"] = str(out_path)

    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        log.error("Failed to generate report for %s (%s, %s): %s",
                  fips, county_name, state, exc)

    result["elapsed_ms"] = int((time.monotonic() - start) * 1000)
    return result


def run_batch(
    counties: list[dict[str, Any]],
    db_url: str,
    output_dir: Path,
    report_format: str,
    parallel: int,
) -> list[dict[str, Any]]:
    """Generate reports for all counties, optionally in parallel."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    if parallel <= 1:
        for county in tqdm(counties, desc="Generating reports", unit="county"):
            result = generate_single_report(
                fips=county["county_fips"],
                county_name=county["county_name"],
                state=county["state_abbr"],
                db_url=db_url,
                output_dir=output_dir,
                report_format=report_format,
            )
            results.append(result)
    else:
        futures = {}
        with ProcessPoolExecutor(max_workers=parallel) as pool:
            for county in counties:
                future = pool.submit(
                    generate_single_report,
                    fips=county["county_fips"],
                    county_name=county["county_name"],
                    state=county["state_abbr"],
                    db_url=db_url,
                    output_dir=output_dir,
                    report_format=report_format,
                )
                futures[future] = county["county_fips"]

            with tqdm(total=len(futures), desc="Generating reports", unit="county") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    pbar.update(1)

    return results


def print_summary(results: list[dict[str, Any]], elapsed_total: float) -> None:
    """Print a nice summary of the batch run."""
    ok_count = sum(1 for r in results if r["status"] == "ok")
    err_count = sum(1 for r in results if r["status"] == "error")
    total_ms = sum(r["elapsed_ms"] for r in results)
    avg_ms = total_ms // len(results) if results else 0

    print("\n" + "=" * 60)
    print("  Batch Report Summary")
    print("=" * 60)
    print(f"  Total counties:      {len(results)}")
    print(f"  Successful:          {ok_count}")
    print(f"  Failed:              {err_count}")
    print(f"  Avg time per report: {avg_ms}ms")
    print(f"  Total wall time:     {elapsed_total:.1f}s")

    if err_count > 0:
        print(f"\n  Failed counties:")
        for r in results:
            if r["status"] == "error":
                print(f"    {r['fips']} ({r['county']}, {r['state']}): {r['error']}")

    print("=" * 60)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-generate WellNest county equity reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s                              Generate all county reports
  %(prog)s --state IL --state CA        Only Illinois and California
  %(prog)s --fips 17031                 Just Cook County
  %(prog)s --parallel 4 --output-dir ./reports/output
        """,
    )
    parser.add_argument(
        "--state", action="append", dest="states", metavar="ST",
        help="Filter to specific state(s), e.g. --state IL --state CA",
    )
    parser.add_argument(
        "--fips", action="append", dest="fips_codes", metavar="FIPS",
        help="Filter to specific county FIPS code(s)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="./reports/output",
        help="Directory for generated reports (default: ./reports/output)",
    )
    parser.add_argument(
        "--format", dest="report_format", choices=["pdf"], default="pdf",
        help="Report format (default: pdf)",
    )
    parser.add_argument(
        "--parallel", type=int, default=1,
        help="Number of parallel workers (default: 1, sequential)",
    )
    parser.add_argument(
        "--db-url", type=str, default=None,
        help="PostgreSQL connection URL (default: from DATABASE_URL env or .env)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_url = args.db_url or _get_db_url()
    output_dir = Path(args.output_dir)

    log.info("Fetching county list...")
    counties = fetch_county_list(db_url, states=args.states, fips_codes=args.fips_codes)

    if not counties:
        log.warning("No counties found matching filters. Is the gold.county_summary table populated?")
        log.info("Hint: run 'python scripts/seed_sample_data.py' first to load sample data")
        sys.exit(1)

    log.info(
        "Generating %d reports → %s (parallel=%d)",
        len(counties), output_dir, args.parallel,
    )

    t0 = time.monotonic()
    results = run_batch(
        counties=counties,
        db_url=db_url,
        output_dir=output_dir,
        report_format=args.report_format,
        parallel=args.parallel,
    )
    elapsed = time.monotonic() - t0

    print_summary(results, elapsed)

    err_count = sum(1 for r in results if r["status"] == "error")
    if err_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

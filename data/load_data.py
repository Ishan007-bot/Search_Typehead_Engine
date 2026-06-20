"""
Ingest an e-commerce search-queries CSV into SQLite.

Usage:
    python data/load_data.py                      # loads data/queries.csv
    python data/load_data.py path/to/kaggle.csv   # loads any e-commerce CSV
    python data/load_data.py kaggle.csv --reset   # wipe DB first, then load

Dataset (intended source for submission):
    Kaggle e-commerce search-queries dataset. The full file is NOT committed to
    the repo (it is large and requires a Kaggle login). The committed
    data/queries.csv is a small representative sample in the same schema so the
    project runs offline immediately. Drop the full Kaggle CSV in and re-run this
    loader with no code changes.

Schema normalization (why this exists):
    Different e-commerce datasets name their columns differently
    (search_term / query / keyword ; count / frequency / popularity / searches).
    This loader maps any of those to our canonical (query, count). If the file
    has NO count column at all, it AGGREGATES rows by query and uses the row
    frequency as the count -- i.e. it "derives counts by aggregation", which is
    explicitly allowed by the assignment's dataset requirement (section 3).
"""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

# Make `import app.datastore` work when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.datastore import DataStore, DEFAULT_DB_PATH  # noqa: E402

# Candidate column names we will accept, in priority order.
QUERY_COLS = ["query", "search_term", "search_query", "keyword", "term", "title", "product_name", "name"]
COUNT_COLS = ["count", "frequency", "freq", "popularity", "searches", "search_count", "views", "rating_count"]

DEFAULT_CSV = Path(__file__).resolve().parent / "queries.csv"


def _pick_column(header: list[str], candidates: list[str]) -> str | None:
    """Find the first candidate column present in the header (case-insensitive)."""
    lower = {h.lower().strip(): h for h in header}
    for cand in candidates:
        if cand in lower:
            return lower[cand]
    return None


def parse_csv(csv_path: Path) -> list[tuple[str, int]]:
    """Return [(query, count), ...] from any supported e-commerce CSV layout."""
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} appears to be empty or has no header row.")

        query_col = _pick_column(reader.fieldnames, QUERY_COLS)
        count_col = _pick_column(reader.fieldnames, COUNT_COLS)

        if query_col is None:
            raise ValueError(
                f"Could not find a query column in {csv_path}. "
                f"Headers were: {reader.fieldnames}. "
                f"Expected one of: {QUERY_COLS}"
            )

        if count_col is not None:
            # Counts present -> read them directly.
            rows: list[tuple[str, int]] = []
            for row in reader:
                q = (row.get(query_col) or "").strip().lower()
                if not q:
                    continue
                raw = (row.get(count_col) or "0").strip()
                try:
                    c = int(float(raw))
                except ValueError:
                    c = 0
                if c > 0:
                    rows.append((q, c))
            print(f"  columns: query='{query_col}', count='{count_col}'")
            return rows

        # No count column -> derive counts by aggregation (one search per row).
        print(f"  columns: query='{query_col}', count=<derived by aggregation>")
        counter: Counter[str] = Counter()
        for row in reader:
            q = (row.get(query_col) or "").strip().lower()
            if q:
                counter[q] += 1
        return list(counter.items())


def main() -> None:
    parser = argparse.ArgumentParser(description="Load an e-commerce CSV into SQLite.")
    parser.add_argument("csv", nargs="?", default=str(DEFAULT_CSV),
                        help="Path to the CSV (default: data/queries.csv)")
    parser.add_argument("--reset", action="store_true",
                        help="Delete the existing SQLite DB before loading.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    if args.reset and DEFAULT_DB_PATH.exists():
        DEFAULT_DB_PATH.unlink()
        print(f"Reset: removed {DEFAULT_DB_PATH}")

    print(f"Loading {csv_path} ...")
    rows = parse_csv(csv_path)
    if not rows:
        print("No valid rows found.", file=sys.stderr)
        sys.exit(1)

    store = DataStore()
    written = store.upsert_many(rows)
    total = store.count_queries()
    store.close()

    print(f"Done. Ingested {written} rows. Total distinct queries in DB: {total}")
    print(f"DB: {DEFAULT_DB_PATH}")


if __name__ == "__main__":
    main()

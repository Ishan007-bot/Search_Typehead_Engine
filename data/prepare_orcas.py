"""
Turn the raw Microsoft/TREC ORCAS dataset into the `query,count` CSV this project
loads. ORCAS = real Bing search queries mapped to clicked documents — genuine
search-query data with click frequency, which is the strongest possible fit for a
search typeahead (and great provenance for the viva).

WHY THIS SCRIPT EXISTS
    The raw ORCAS file is large and one row PER CLICK, not per query. We aggregate
    clicks into a per-query frequency, filter obvious junk, and keep the top-N most
    frequent queries so the repo and the in-memory trie stay a sensible size.

GET THE DATA (do this once, locally — the file is large):
    The ORCAS download lives on the TREC Deep Learning / MS MARCO pages. Look for
    "ORCAS" -> a file named like `orcas.tsv` or `orcas.tsv.gz` (~ a few GB). Save it
    into this `data/` folder, e.g. data/orcas.tsv (gunzip if needed).

RUN:
    python data/prepare_orcas.py data/orcas.tsv --top 200000
    python data/load_data.py --reset          # load the new queries.csv into SQLite

FORMAT HANDLING (robust to ORCAS variants):
    - Click-log form: columns  query_id <tab> query <tab> doc_id <tab> doc_url
      -> we count ROWS per query (each row = one click) as the frequency.
    - Pre-counted form: a `query` column + a `count`/`clicks`/`freq` column
      -> we use that column directly.
    Header may or may not be present; we detect both.
"""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

OUT = Path(__file__).resolve().parent / "queries.csv"

COUNT_HEADERS = {"count", "clicks", "freq", "frequency"}
QUERY_HEADERS = {"query", "search_query", "q"}

# Light junk filter for raw query logs.
def _is_junk(q: str) -> bool:
    if not q or len(q) < 2 or len(q) > 100:
        return True
    if not any(c.isalnum() for c in q):
        return True
    # drop queries that are mostly a URL
    if q.startswith("http") or "www." in q:
        return True
    return False


def _sniff(path: Path):
    """Return (delimiter, has_header, query_idx, count_idx_or_None)."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        first = f.readline()
    delim = "\t" if first.count("\t") >= first.count(",") else ","
    cols = [c.strip().lower() for c in first.rstrip("\n").split(delim)]
    has_header = any(c in QUERY_HEADERS for c in cols) or any(c in COUNT_HEADERS for c in cols)
    query_idx, count_idx = None, None
    if has_header:
        for i, c in enumerate(cols):
            if c in QUERY_HEADERS and query_idx is None:
                query_idx = i
            if c in COUNT_HEADERS and count_idx is None:
                count_idx = i
        if query_idx is None:
            query_idx = 1 if len(cols) > 1 else 0
    else:
        # No header. ORCAS click-log: id, query, doc_id, url -> query at index 1.
        query_idx = 1 if len(cols) >= 2 else 0
    return delim, has_header, query_idx, count_idx


def main() -> None:
    ap = argparse.ArgumentParser(description="Aggregate raw ORCAS into query,count.")
    ap.add_argument("path", help="Path to the raw ORCAS .tsv/.csv file")
    ap.add_argument("--top", type=int, default=200_000,
                    help="Keep the top-N queries by frequency (default 200k).")
    args = ap.parse_args()

    src = Path(args.path)
    if not src.exists():
        print(f"ERROR: file not found: {src}", file=sys.stderr)
        sys.exit(1)

    delim, has_header, q_idx, c_idx = _sniff(src)
    print(f"Detected: delimiter={delim!r}, header={has_header}, "
          f"query_col={q_idx}, count_col={c_idx if c_idx is not None else '<derive by counting rows>'}")

    counts: Counter[str] = Counter()
    rows_read = 0
    with open(src, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        if has_header:
            next(reader, None)
        for row in reader:
            rows_read += 1
            if len(row) <= q_idx:
                continue
            q = row[q_idx].strip().lower()
            if _is_junk(q):
                continue
            if c_idx is not None and len(row) > c_idx:
                try:
                    counts[q] += int(float(row[c_idx]))
                except ValueError:
                    counts[q] += 1
            else:
                counts[q] += 1            # one row = one click
            if rows_read % 1_000_000 == 0:
                print(f"  ...{rows_read:,} rows read, {len(counts):,} distinct queries")

    if not counts:
        print("No queries parsed — check the file format.", file=sys.stderr)
        sys.exit(1)

    top = counts.most_common(args.top)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "count"])
        w.writerows(top)

    print(f"\nWrote {len(top):,} rows to {OUT}")
    print(f"(from {rows_read:,} raw rows, {len(counts):,} distinct queries before top-N)")
    print("Top 10 by frequency:")
    for q, c in top[:10]:
        print(f"  {c:>9,}  {q}")
    print("\nNext: python data/load_data.py --reset")


if __name__ == "__main__":
    main()

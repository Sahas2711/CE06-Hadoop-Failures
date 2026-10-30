#!/usr/bin/env python3
"""CE-06 Hadoop Practical - NASA HTTP log dataset preparation.

Inputs : dataset/original/NASA_access_log_Jul95.gz, NASA_access_log_Aug95.gz
Outputs: dataset/clean/*.log          - well-formed records only
         dataset/malformed/*.log      - same data + INJECTED malformed records
         dataset/README.md            - statistics
         dataset/stats.json           - machine-readable stats

Injected malformed records are deterministic (seed=42) and carry the marker
"CE06_BAD_RECORD" so the mapper can detect them exactly.

NASA combined log format (per ita.ee.lbl.gov):
  host ident authuser [date] "request" status bytes
Example:
  125.72.107.109 - - [01-Jul-1995:00:00:01 -0400] "GET /shuttle/news/countdown... HTTP/1.0" 200 6245
"""
import gzip
import json
import os
import random
import re
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORIGINAL = os.path.join(BASE, "dataset", "original")
CLEAN = os.path.join(BASE, "dataset", "clean")
MALFORMED = os.path.join(BASE, "dataset", "malformed")

SEED = 42
# Parse rule: host ... [date] "request" status bytes
GOOD_RE = re.compile(
    r'^(\S+) \S+ \S+ \[[^\]]+\] "\S+ [^"]*?" \d{3} (\d+|-)$'
)

# Deterministic malformed record types (all contain CE06_BAD_RECORD marker)
BAD_TEMPLATES = [
    'CE06_BAD_RECORD this line is not a valid log entry at all',
    '{broken_json_host - - [not-a-date] "GARBAGE status 99999',
    'CE06_BAD_RECORD host=="?" [====] "????" xx yy zz EXTRA',
    '- - - [01-Jul-1995:99:99:99 -0400] "UNKNOWN /none HTTP/9.9" 200',   # bad status len handled: still marker
    'CE06_BAD_RECORD',
]


def gunzip_lines(path):
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield line.rstrip("\n")


def main():
    os.makedirs(CLEAN, exist_ok=True)
    os.makedirs(MALFORMED, exist_ok=True)

    random.seed(SEED)
    files = sorted(
        os.path.join(ORIGINAL, f) for f in os.listdir(ORIGINAL)
        if f.endswith(".gz")
    )
    if not files:
        print("FATAL: no .gz files in dataset/original - run download_dataset.sh first")
        sys.exit(1)

    total = clean = 0
    status_counts = {}
    per_file_stats = []
    sample_bad_naturally = []

    clean_path = os.path.join(CLEAN, "nasa_access_clean.log")
    with open(clean_path, "w", encoding="utf-8") as out:
        for src in files:
            f_total = f_clean = f_malformed_natural = 0
            for line in gunzip_lines(src):
                f_total += 1
                total += 1
                if GOOD_RE.match(line):
                    f_clean += 1
                    clean += 1
                    out.write(line + "\n")
                    parts = line.rsplit(" ", 2)
                    st = parts[-2]
                    status_counts[st] = status_counts.get(st, 0) + 1
                else:
                    f_malformed_natural += 1
                    if len(sample_bad_naturally) < 5:
                        sample_bad_naturally.append(line[:160])
            per_file_stats.append({
                "file": os.path.basename(src),
                "records": f_total,
                "well_formed": f_clean,
                "naturally_malformed": f_malformed_natural,
            })
            print(f"{os.path.basename(src)}: {f_total} records, "
                  f"{f_clean} clean, {f_malformed_natural} naturally malformed")

    # Build malformed dataset: clean records + injected bad records
    rng = random.Random(SEED)
    mal_path = os.path.join(MALFORMED, "nasa_access_malformed.log")
    injected = 0
    # inject evenly: every Nth line
    inject_every = max(1, clean // 20)  # ~20 injected records
    with open(clean_path, "r", encoding="utf-8") as src, \
            open(mal_path, "w", encoding="utf-8") as out:
        for i, line in enumerate(src, 1):
            out.write(line + "\n")
            if i % inject_every == 0:
                bad = BAD_TEMPLATES[injected % len(BAD_TEMPLATES)]
                out.write(bad + "\n")
                injected += 1

    stats = {
        "seed": SEED,
        "source_files": per_file_stats,
        "total_records": total,
        "well_formed": clean,
        "naturally_malformed": total - clean,
        "injected_malformed": injected,
        "status_code_distribution": dict(sorted(status_counts.items())),
        "clean_file": os.path.relpath(clean_path, BASE),
        "malformed_file": os.path.relpath(mal_path, BASE),
        "naturally_malformed_samples": sample_bad_naturally,
    }
    with open(os.path.join(BASE, "dataset", "stats.json"), "w") as fh:
        json.dump(stats, fh, indent=2)

    readme = f"""# CE-06 Dataset - NASA HTTP Logs (Kennedy Space Center)

## Source
- https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz ({os.path.getsize(os.path.join(ORIGINAL,'NASA_access_log_Jul95.gz'))} bytes)
- https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz ({os.path.getsize(os.path.join(ORIGINAL,'NASA_access_log_Aug95.gz'))} bytes)
- Authentic NASA Kennedy Space Center HTTP server logs (Lawrence Berkeley Lab archive).

## Layout
- `original/` - pristine downloaded .gz files (never modified)
- `clean/`    - well-formed records only (`nasa_access_clean.log`)
- `malformed/` - clean records + {injected} INJECTED malformed records
  containing the marker `CE06_BAD_RECORD` (seed={SEED}, reproducible)

## Statistics
| Metric | Value |
|---|---|
| Total records read | {total} |
| Well-formed | {clean} |
| Naturally malformed (skipped) | {total - clean} |
| Injected malformed (in malformed/) | {injected} |
| Log format | NASA combined (host ident authuser [date] \"req\" status bytes) |

## Injected malformed record types
```
{chr(10).join(BAD_TEMPLATES)}
```

## Status code distribution (clean dataset)
{chr(10).join(f'- {k}: {v}' for k, v in sorted(status_counts.items()))}

## Reproducibility
seed={SEED}; regenerate with: `python3 scripts/prepare_dataset.py`
"""
    with open(os.path.join(BASE, "dataset", "README.md"), "w") as fh:
        fh.write(readme)

    print(f"\nTOTAL={total} CLEAN={clean} INJECTED={injected}")
    print(f"clean file     : {clean_path} ({os.path.getsize(clean_path)} bytes)")
    print(f"malformed file : {mal_path} ({os.path.getsize(mal_path)} bytes)")
    print("STATUS_DIST=" + json.dumps(dict(sorted(status_counts.items()))))


if __name__ == "__main__":
    main()

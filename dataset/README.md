# CE-06 Dataset - NASA HTTP Logs (Kennedy Space Center)

## Source
- https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz (20676672 bytes)
- https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz (16633316 bytes)
- Authentic NASA Kennedy Space Center HTTP server logs (Lawrence Berkeley Lab archive).

## Layout
- `original/` - pristine downloaded .gz files (never modified)
- `clean/`    - well-formed records only (`nasa_access_clean.log`)
- `malformed/` - clean records + 20 INJECTED malformed records
  containing the marker `CE06_BAD_RECORD` (seed=42, reproducible)

## Statistics
| Metric | Value |
|---|---|
| Total records read | 3461613 |
| Well-formed | 3461565 |
| Naturally malformed (skipped) | 48 |
| Injected malformed (in malformed/) | 20 |
| Log format | NASA combined (host ident authuser [date] "req" status bytes) |

## Injected malformed record types
```
CE06_BAD_RECORD this line is not a valid log entry at all
{broken_json_host - - [not-a-date] "GARBAGE status 99999
CE06_BAD_RECORD host=="?" [====] "????" xx yy zz EXTRA
- - - [01-Jul-1995:99:99:99 -0400] "UNKNOWN /none HTTP/9.9" 200
CE06_BAD_RECORD
```

## Status code distribution (clean dataset)
- 200: 3100521
- 302: 73070
- 304: 266773
- 403: 224
- 404: 20871
- 500: 65
- 501: 41

## Reproducibility
seed=42; regenerate with: `python3 scripts/prepare_dataset.py`

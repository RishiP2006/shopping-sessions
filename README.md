# Shopping Sessions: native C++ pipeline

A single static C++ binary (`src/pipeline.cpp`, `src/parquet.hpp`) in an empty (`FROM scratch`) Docker
image, about 1.5 MB. It implements `rules.md` and writes `/output/user_daily/*.parquet` and
`/output/campaign_daily/*.parquet` with a small built-in Parquet writer (no libraries).

## How it works

1. **Parse** (all threads). Files are read with `pread` in 4 MB chunks into 256 KB buffers that stay in
   cache. A SIMD scanner (AVX2 on x86) builds comma/quote/newline bitmasks per 64-byte block and cuts
   each line with a fixed number of bit operations. Quotes are validated with mask arithmetic.
   Timestamps use SWAR (all three forms). IDs up to 9 ASCII chars pack exactly into 63 bits, so
   numeric order equals byte order; anything else is interned.
   Links are parsed first into a lock-free hash table, and one thread runs union-find (each person is
   named by the smallest ID) while the others keep parsing.
2. **Dedup.** Every event copy writes a 16-byte key `(event_id, ingest, ref)` partitioned by event_id.
   Per partition, the copy with the smallest `ingest_ts` wins and losing copies are marked in a bitmap.
3. **Map.** Valid payloads are partitioned by user with the same hash bits as the link table, so each
   partition's user→person lookups hit a small cache-resident slice. Events and clicks are then
   scattered by person.
4. **Sessions** (per person partition). Group by person, radix-sort by timestamp, split on gaps over
   30 min, credit the latest click within 7 days (ties → smallest click_id), and count assists lazily
   (a campaign is credited when it leaves the window or the day changes).
5. **Write.** Each thread writes its own `user_daily` part as soon as it finishes; the last thread
   writes `campaign_daily`.

## Testing

- `make native-fixtures`: the 36 edge-case fixtures.
- Dev set: `make run-dev DEV=path/to/dev && make check-dev ANSWER=path/to/dev-answer`
- `tools/reference.py`: a slow, literal reference implementation. `tools/cross.sh` checks other
  generator seeds against it; `tools/torture.sh` checks randomized adversarial inputs (long lines,
  bad quoting, `\r`, unicode, missing final newline, header-only files). `SS_RDBUF` / `SS_CHUNK` env
  vars shrink buffers to exercise boundary handling.
- The `bench` branch has a GitHub Actions workflow (`.github/workflows/bench.yml`). It builds the real
  image, runs fixtures and the dev set with the scorer's flags, and A/B-times the variants listed in
  `.github/variants` on 20M events.

Opt-in experiments (compile flags, all slower or neutral on x86): `SINGLE`, `PARSE_LOOKUP`, `USE_WC`,
`USE_MMAP`, `NEW_SCAN`, `HOURCACHE`, `SESS_PERSORT`, `PROF_STEPS` (per-step timers).

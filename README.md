# Shopping Sessions: native C++ pipeline

A single static C++ binary (`src/pipeline.cpp`, `src/parquet.hpp`) in an empty Docker image.
It implements `rules.md` and writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet`.

- Build: `make build` (linux/amd64) or `make native` (local binary in `build/`)
- Fixtures: `make fixtures` or `make native-fixtures`
- Dev set: `make run-dev DEV=path/to/dev && make check-dev ANSWER=path/to/dev-answer`
- `tools/reference.py` is a slow literal reference used to cross-check other seeds (`tools/cross.sh`)
  and randomized edge cases (`tools/torture.sh`).

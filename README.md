# Exchange Rate Data Pipeline



A dbt-based pipeline that ingests Open Exchange rates (via API → Fivetran → GCS → BigQuery), applies transformations, and exposes analytical models.

## Quick links
- Project config: [dbt_project.yml](dbt_project.yml)  
- Package manifest: [packages.yml](packages.yml)  
- Current README: [README.md](README.md)  
- Source config (Open Exchange): [`models/base/_src_open_exchange.yml`](models/base/_src_open_exchange.yml)  
- Rate docs: [docs/rates.md](docs/rates.md)  
- Example test: [tests/test_negative_rates_in_raw.sql](tests/test_negative_rates_in_raw.sql)  
- dbt logs: [logs/dbt.log](logs/dbt.log)  
- Build artifacts: [target/manifest.json](target/manifest.json), [target/run_results.json](target/run_results.json), [target/graph_summary.json](target/graph_summary.json)

## Overview
This repository implements a pipeline that:
1. Pulls exchange rate data from Open Exchange (via Fivetran)
2. Stores raw data as Parquet in Google Cloud Storage
3. Loads data into BigQuery
4. Transforms data with dbt into three logical layers: base, prep, and dimensional

## Project layout
- [models/](models/) — dbt models (layers: `base/`, `prep/`, `dimensional/`)  
- [macros/](macros/) — dbt macros  
- [seeds/](seeds/) — project seeds (currently empty `.gitkeep`)  
- [snapshots/](snapshots/) — snapshot configs  
- [analyses/](analyses/) — analysis queries  
- [tests/](tests/) — SQL tests (example: [`tests/test_negative_rates_in_raw.sql`](tests/test_negative_rates_in_raw.sql))  
- [docs/](docs/) — documentation pages (example: [`docs/rates.md`](docs/rates.md))  
- [logs/](logs/) — runtime logs (dbt, queries, stdout/stderr)  
- [target/](target/) — dbt compiled/artifact outputs (manifest, run_results, graph)

## dbt transformation layers
- Base models: 1:1 mapping to source tables (see [`models/base/_src_open_exchange.yml`](models/base/_src_open_exchange.yml)).  
- Prep models: light cleaning and canonicalization.  
- Dimensional models: business-ready tables for reporting.

## Prerequisites
- dbt installed and configured for BigQuery (profiles must be set up outside this repo).  
- gcloud auth configured if interacting with GCS/BigQuery CLI.  
- Packages: run `dbt deps` to install packages declared in [packages.yml](packages.yml).

## Common commands
Run from repo root:

```sh
# Install dbt packages
dbt deps

# Run all models
dbt run

# Run a specific model or folder (example: models/dimensional)
dbt run -m dimensional

# Run tests (schema & generic tests)
dbt test

# Generate docs and serve locally
dbt docs generate
dbt docs serve

# Run snapshots
dbt snapshot
```

If you rely on seeds (none committed currently), run:
```sh
dbt seed
```

## Testing and CI
- Unit/analytic tests live in [tests/](tests/). Example: [`tests/test_negative_rates_in_raw.sql`](tests/test_negative_rates_in_raw.sql).  
- dbt runtime artifacts are written to [target/](target/) — inspect [`target/run_results.json`](target/run_results.json) and [`target/manifest.json`](target/manifest.json) after runs.

## Troubleshooting
- Check dbt logs in [logs/dbt.log](logs/dbt.log).  
- Review compiled SQL and manifest in [target/](target/) for lineage and debugging.

## Notes for contributors
- Follow dbt conventions: place source YAML in `models/base/` (see [`models/base/_src_open_exchange.yml`](models/base/_src_open_exchange.yml)).  
- Add tests to [tests/](tests/) when adding or changing models.  
- Update docs in [docs/](docs/) for any business logic changes.

## References
- Source YAML: [`models/base/_src_open_exchange.yml`](models/base/_src_open_exchange.yml)  
- Rates doc: [`docs/rates.md`](docs/rates.md)

## Contact
For repo-specific questions, open an issue or contact the repository maintainer.
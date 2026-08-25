# Repository Guidelines

## Project Structure & Module Organization
This repository implements a two-stage pipeline. `search_group/` collects data and writes markdown into `data/md/`. `process_group/` reads `data/md/` and generates reports in `data/reports/`. Root watchlists live at `StockID_TWSE_TPEX.csv` and `StockID_TWSE_TPEX_focus.csv`. Utility scripts such as `verify_stocks.py` and `scripts/quarantine_files.py` handle validation and cleanup. Configuration artifacts live in `configs/`.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` installs runtime dependencies.
- `python search_group/search_cli.py validate` verifies API keys and search configuration.
- `python search_group/search_cli.py search --all --count 2 --min-quality 4` runs a full search pass.
- `python process_group/process_cli.py validate` checks the processing stack.
- `python process_group/process_cli.py process` generates all reports and uploads if configured.
- `python process_group/process_cli.py keyword-summary --no-upload` or `watchlist-summary --no-upload` runs local analysis without Sheets upload.

## Coding Style & Naming Conventions
Use Python 3.8+, 4-space indentation, and PEP 8 style. Modules and functions use `snake_case`; classes use `CapWords`. CLI entry points end with `_cli.py`. Generated MD files follow `{code}_{company}_{source}_{hash}.md` in `data/md/`. Reports are CSVs in `data/reports/` with `*_summary` and `*_latest` naming.

## Testing Guidelines
There is no dedicated test suite. Use the CLI `validate` commands and a small single-company run before large jobs, for example: `python search_group/search_cli.py search --company 2330 --count 1`. Lint/test tools are listed in `requirements.txt` but commented out; enable locally if needed.

## Commit & Pull Request Guidelines
Commit history commonly uses an emoji prefix and a descriptive title with version/date. Follow the same pattern (without rewriting history), for example: `Search Group v3.5.0-pure-hash Results - 2025-12-23 18:52:53`. Pull requests should include a short summary, commands run, and any updated report artifacts; link related issues when applicable.

## Security & Configuration Tips
Store API keys and Google Sheets credentials in a local `.env` file and never commit secrets. Generated data under `data/` can be large and noisy; only commit report outputs when they are the intended deliverable.

# amptec-scraper

Single-module Python scraper to extract product metadata and images from `https://amptec.com`.
Outputs one folder per product containing `metadata.json` and downloaded images.

## Features

- Crawls the domain and discovers product pages via JSON-LD `Product`, OpenGraph, and heuristics.
- Extracts title, SKU, price, description (HTML + text), categories/breadcrumbs when available, images, and source URL.
- Writes `data/products/<slug>/metadata.json` and saves images beside it.
- Respects `robots.txt` by default and rate-limits requests.
- Idempotent: skips existing product folders unless `--force` is provided.

## Quickstart

```bash
git clone <your-repo-url>.git
cd amptec-scraper
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m amptec_scraper --base-url https://amptec.com --out data/products --concurrency 6
```

Options:
```bash
python -m amptec_scraper --help
```

## Structure

```
amptec-scraper/
  README.md
  LICENSE
  requirements.txt
  pyproject.toml
  .gitignore
  .env.example
  configs/
    scraper.config.json
  src/
    amptec_scraper/
      __init__.py
      __main__.py
      main.py
      crawl.py
      parse.py
      store.py
      download.py
      utils.py
      agent_instructions.json
  scripts/
    run_scrape.sh
  tests/
    test_slugify.py
```

## Notes

- This code does not run here due to disabled outbound network. Run locally.
- Honor the target site's Terms of Service and robots.txt.
- For high-volume usage, prefer polite delays and caching; consider Playwright if pages are JS-heavy.

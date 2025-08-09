#!/usr/bin/env bash
set -euo pipefail
python -m amptec_scraper --base-url https://amptec.com --out data/products --concurrency 6

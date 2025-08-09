import os, json, argparse, asyncio, time
from urllib.parse import urlparse
from scrapingbee import ScrapingBeeClient
from bs4 import BeautifulSoup
from tqdm import tqdm
from .crawl import crawl_with_scrapingbee
from .parse import parse_product
from .download import download_images_with_scrapingbee, download_documents_with_scrapingbee
from .store import write_product
from .utils import guess_product_slug, ensure_dir, canonical_url

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

async def fetch_html_with_scrapingbee(client, url, timeout=30, max_retries=3):
    """Fetch HTML using ScrapingBee API with retry logic for 503 errors"""
    for attempt in range(max_retries):
        try:
            # Use different proxy settings on retries
            proxy_params = {
                'render_js': 'false',
                'premium_proxy': 'true',
                'country_code': 'us'
            }
            
            # On retry attempts, use different proxy configurations
            if attempt > 0:
                proxy_params.update({
                    'premium_proxy': 'true',
                    'country_code': ['us', 'ca', 'gb'][attempt % 3],  # Rotate countries
                    'session_id': f"session_{attempt}_{hash(url) % 1000}"  # Session rotation
                })
                print(f"   🔄 Retry attempt {attempt + 1}/{max_retries} with rotated proxy")
            
            response = client.get(url, params=proxy_params)
            
            if response.ok:
                return response.content.decode('utf-8')
            elif response.status_code == 503:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 5  # Exponential backoff + base wait
                    print(f"   ⚠️  503 Service Unavailable, waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"ScrapingBee 503 error after {max_retries} attempts")
            else:
                raise Exception(f"ScrapingBee error: {response.status_code} - {response.text}")
                
        except Exception as e:
            if attempt == max_retries - 1:
                raise Exception(f"ScrapingBee request failed for {url} after {max_retries} attempts: {str(e)}")
            else:
                wait_time = 2 ** attempt
                print(f"   💥 Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                print(f"   ⏱️  Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)

async def process_product(scrapingbee_client, url, base_url, out_root, concurrency, timeout, image_max_bytes):
    from .parse import extract_associated_products, extract_document_links
    import json
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Add delay between attempts
            if attempt > 0:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

            html = await fetch_html_with_scrapingbee(scrapingbee_client, url, timeout=timeout)
            meta = parse_product(html, url)
            slug = guess_product_slug(meta.get("title") or "", url)
            product_dir = os.path.join(out_root, slug)
            ensure_dir(product_dir)
            images_saved = await download_images_with_scrapingbee(scrapingbee_client, base_url, meta.get("images", []), product_dir, concurrency=max(1, concurrency//2), timeout=timeout, max_bytes=image_max_bytes)
            write_product(out_root, slug, meta, images_saved)
            
            # Extract and save associated products
            print(f"🔗 Extracting associated products...")
            associated_products = extract_associated_products(html, base_url)
            if associated_products:
                print(f"   Found {len(associated_products)} associated products")
                associated_file = os.path.join(product_dir, "associated_products.json")
                with open(associated_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "source_url": url,
                        "extracted_at": time.time(),
                        "associated_products": associated_products
                    }, f, indent=2, ensure_ascii=False)
            else:
                print(f"   No associated products found")
            
            # Extract and download product documentation
            print(f"📄 Extracting product documentation...")
            doc_links = extract_document_links(html, base_url)
            if doc_links:
                print(f"   Found {len(doc_links)} documentation files")
                docs_dir = os.path.join(product_dir, "documentation")
                ensure_dir(docs_dir)
                docs_saved = await download_documents_with_scrapingbee(scrapingbee_client, doc_links, docs_dir, timeout=timeout)
                
                # Save documentation metadata
                docs_file = os.path.join(product_dir, "documentation.json")
                with open(docs_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "source_url": url,
                        "extracted_at": time.time(),
                        "documentation": docs_saved
                    }, f, indent=2, ensure_ascii=False)
                
                print(f"   ✅ Downloaded {len([d for d in docs_saved if d.get('status') == 'downloaded'])} documents")
            else:
                print(f"   No documentation found")
            
            return slug
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            print(f"  Retrying {url} (attempt {attempt + 2}/{max_retries}) after error: {str(e)}")
            continue

async def run(args):
    print("Starting amptec scraper with ScrapingBee...")
    cfg = load_config(args.config) if args.config else {}
    base_url = args.base_url or cfg.get("base_url")
    out_root = args.out or cfg.get("output_dir", "data/products")
    concurrency = int(args.concurrency or cfg.get("max_concurrency", 1))
    timeout = int(args.timeout or cfg.get("timeout_seconds", 30))
    image_max_bytes = int(cfg.get("image_max_bytes", 50_000_000))
    keywords = cfg.get("product_url_keywords", ["product"])
    allowed_domains = cfg.get("allowed_domains", [urlparse(base_url).netloc])

    # Get ScrapingBee API key from environment
    scrapingbee_api_key = os.getenv('SCRAPINGBEE_API_KEY')
    if not scrapingbee_api_key:
        print("Error: SCRAPINGBEE_API_KEY environment variable is required")
        return

    scrapingbee_client = ScrapingBeeClient(api_key=scrapingbee_api_key)

    print(f"Configuration:")
    print(f"  Base URL: {base_url}")
    print(f"  Output directory: {out_root}")
    print(f"  Concurrency: {concurrency}")
    print(f"  Timeout: {timeout}s")
    print(f"  Keywords: {keywords}")
    print(f"  Allowed domains: {allowed_domains}")
    print(f"  Using ScrapingBee API")

    ensure_dir(out_root)

    # Discover product URLs
    print(f"\n🔍 Starting product URL discovery...")
    print(f"🎯 Target: {base_url}")
    try:
        product_urls = await crawl_with_scrapingbee(scrapingbee_client, base_url, allowed_domains, keywords, concurrency=concurrency, timeout=timeout)
        product_urls = sorted(set(product_urls))
        print(f"\n✅ URL Discovery Complete!")
        print(f"   📊 Found {len(product_urls)} unique product URLs")
        
        if product_urls:
            print(f"   📋 Product URLs found:")
            for i, url in enumerate(product_urls[:10], 1):  # Show first 10
                print(f"      {i}. {url}")
            if len(product_urls) > 10:
                print(f"      ... and {len(product_urls) - 10} more")
    except Exception as e:
        print(f"❌ Error during crawling: {e}")
        product_urls = []

    # Fallback: include root if nothing discovered
    if not product_urls:
        print("No product URLs found, using base URL as fallback")
        product_urls = [base_url]

    print(f"\n🛒 Starting product processing phase...")
    print(f"📊 Processing {len(product_urls)} product pages")
    print(f"⏱️  Delay: 1 second between requests")
    
    results = []
    for i, url in enumerate(tqdm(product_urls, desc="Scraping products")):
        try:
            print(f"\n📦 [{i+1}/{len(product_urls)}] Processing product: {url}")
            
            # Add delay between requests to be more polite
            if i > 0:
                await asyncio.sleep(1)  # 1 second delay between requests

            print(f"🚀 Starting product extraction...")
            slug = await process_product(scrapingbee_client, url, base_url, out_root, concurrency, timeout, image_max_bytes)
            results.append({"url": url, "slug": slug})
            print(f"✅ Successfully processed: {slug}")
            
        except Exception as e:
            print(f"❌ Failed to process {url}")
            print(f"   Error: {str(e)}")
            results.append({"url": url, "error": str(e)})
            continue

    # Write crawl summary
    summary_path = os.path.join(out_root, "_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({"base_url": base_url, "count": len(results), "items": results, "timestamp": time.time()}, f, indent=2)

    print(f"\nScraping complete! Results saved to {summary_path}")
    print(f"Processed {len(results)} items")

def cli():
    p = argparse.ArgumentParser(description="Scrape product metadata and images into per-product folders")
    p.add_argument("--base-url", default=None, help="Base URL to crawl, e.g., https://amptec.com")
    p.add_argument("--out", default="data/products", help="Output root directory")
    p.add_argument("--concurrency", default=6, type=int, help="Max concurrent requests")
    p.add_argument("--timeout", default=30, type=int, help="Request timeout seconds")
    p.add_argument("--config", default="configs/scraper.config.json", help="Path to JSON config")
    args = p.parse_args()
    asyncio.run(run(args))
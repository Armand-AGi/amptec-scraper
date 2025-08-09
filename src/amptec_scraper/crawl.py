import asyncio, re, os
from urllib.parse import urlparse
import httpx
from scrapingbee import ScrapingBeeClient
from bs4 import BeautifulSoup
from .utils import canonical_url, is_same_domain

DEFAULT_HEADERS = {
    "User-Agent": "amptec-scraper/0.1 (+https://example.com)"
}

async def crawl_with_scrapingbee(scrapingbee_client, base_url, allowed_domains, keywords, concurrency=1, timeout=30, max_pages=100):
    """Crawl the base URL to discover product pages using ScrapingBee"""
    print(f"🕷️  Starting ScrapingBee crawl of {base_url}")
    print(f"📋 Crawl parameters:")
    print(f"   - Max pages: {max_pages}")
    print(f"   - Allowed domains: {allowed_domains}")
    print(f"   - Product keywords: {keywords}")
    print(f"   - Timeout: {timeout}s")
    
    visited = set()
    to_visit = {base_url}
    product_urls = set()
    
    while to_visit and len(visited) < max_pages:
        # Process URLs one by one to avoid overwhelming ScrapingBee
        url = to_visit.pop()
        
        if url not in visited:
            visited.add(url)
            try:
                print(f"\n🌐 [{len(visited)}/{max_pages}] Crawling: {url}")
                
                # Use retry logic with rotating proxies for crawling
                max_retries = 3
                response = None
                
                for attempt in range(max_retries):
                    try:
                        proxy_params = {
                            'render_js': 'false',
                            'premium_proxy': 'true',
                            'country_code': 'us'
                        }
                        
                        # Rotate proxy settings on retries
                        if attempt > 0:
                            proxy_params.update({
                                'country_code': ['us', 'ca', 'gb'][attempt % 3],
                                'session_id': f"crawl_{attempt}_{hash(url) % 1000}"
                            })
                            print(f"     🔄 Crawl retry attempt {attempt + 1}/{max_retries}")
                        
                        response = scrapingbee_client.get(url, params=proxy_params)
                        
                        if response.ok:
                            break
                        elif response.status_code == 503:
                            if attempt < max_retries - 1:
                                wait_time = (2 ** attempt) + 3
                                print(f"     ⚠️  503 error, waiting {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            break
                            
                    except Exception as e:
                        if attempt == max_retries - 1:
                            print(f"     ❌ All crawl attempts failed: {e}")
                            break
                        await asyncio.sleep(2 ** attempt)
                
                if response.ok:
                    print(f"   ✅ Response: {response.status_code} ({len(response.content)} bytes)")
                    html = response.content.decode('utf-8')
                    links = _extract_links(base_url, html)
                    print(f"   🔗 Found {len(links)} total links")
                    
                    # Filter links to allowed domains
                    filtered_links = set()
                    for link in links:
                        parsed = urlparse(link)
                        if any(domain in parsed.netloc for domain in allowed_domains):
                            filtered_links.add(link)
                    
                    print(f"   ✂️  Filtered to {len(filtered_links)} links in allowed domains")
                    
                    # Identify product URLs
                    new_products = 0
                    new_links_to_visit = 0
                    
                    for link in filtered_links:
                        if _is_product_url(link, keywords):
                            if link not in product_urls:
                                product_urls.add(link)
                                new_products += 1
                                print(f"   🛒 Found product: {link}")
                        
                        # Add new links to visit (but limit depth)
                        if len(visited) < max_pages and link not in visited and link not in to_visit:
                            to_visit.add(link)
                            new_links_to_visit += 1
                    
                    print(f"   📊 Added {new_products} products, {new_links_to_visit} links to queue")
                    print(f"   📈 Total products found: {len(product_urls)}")
                    print(f"   📝 Queue size: {len(to_visit)}")
                    
                    # Add delay to respect ScrapingBee rate limits
                    print(f"   ⏱️  Waiting 2 seconds before next request...")
                    await asyncio.sleep(2)
                    
                else:
                    print(f"   ❌ ScrapingBee error: {response.status_code}")
                    if hasattr(response, 'text') and response.text:
                        print(f"      Error details: {response.text[:200]}")
                    
            except Exception as e:
                print(f"   💥 Failed to crawl {url}: {e}")
    
    print(f"\n🎉 ScrapingBee crawl complete!")
    print(f"   📊 Total pages visited: {len(visited)}")
    print(f"   🛒 Product URLs found: {len(product_urls)}")
    print(f"   📝 Remaining queue: {len(to_visit)}")
    
    return list(product_urls)

async def crawl(base_url, allowed_domains, keywords, concurrency=3, timeout=30, max_pages=100):
    """Crawl the base URL to discover product pages"""
    print(f"Starting crawl of {base_url}")
    
    visited = set()
    to_visit = {base_url}
    product_urls = set()
    
    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, timeout=timeout) as client:
        while to_visit and len(visited) < max_pages:
            # Process URLs in batches
            batch = list(to_visit)[:concurrency]
            to_visit -= set(batch)
            
            tasks = []
            for url in batch:
                if url not in visited:
                    tasks.append(_crawl_page(client, url, base_url, allowed_domains, keywords))
                    visited.add(url)
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Crawl error: {result}")
                        continue
                    
                    new_links, new_products = result
                    product_urls.update(new_products)
                    
                    # Add new links to visit (but limit depth)
                    for link in new_links:
                        if len(visited) < max_pages and link not in visited:
                            to_visit.add(link)
    
    print(f"Crawl complete. Found {len(product_urls)} product URLs")
    return list(product_urls)

async def _crawl_page(client, url, base_url, allowed_domains, keywords):
    """Crawl a single page and return (links, product_urls)"""
    try:
        print(f"Crawling: {url}")
        
        api_key = os.getenv('SCRAPINGBEE_API_KEY')
        if not api_key:
            raise ValueError("SCRAPINGBEE_API_KEY environment variable is required")
            
        sb_client = ScrapingBeeClient(api_key=api_key)
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: sb_client.get(url, params={
                'render_js': 'false',
                'premium_proxy': 'true',
                'country_code': 'us'
            })
        )
        
        if response.status_code != 200:
            raise Exception(f"ScrapingBee error {response.status_code}")
            
        html = response.text
        links = _extract_links(base_url, html)
        
        # Filter links to allowed domains
        filtered_links = set()
        for link in links:
            parsed = urlparse(link)
            if any(domain in parsed.netloc for domain in allowed_domains):
                filtered_links.add(link)
        
        # Identify product URLs
        product_urls = set()
        for link in filtered_links:
            if _is_product_url(link, keywords):
                product_urls.add(link)
        
        return filtered_links, product_urls
        
    except Exception as e:
        print(f"Failed to crawl {url}: {e}")
        return set(), set()

def _is_product_url(url, keywords):
    """Check if URL likely contains a product based on keywords"""
    url_lower = url.lower()
    return any(keyword in url_lower for keyword in keywords)

def _extract_links(base_url, html):
    try:
        soup = BeautifulSoup(html, "lxml")
        links = set()
        anchor_tags = soup.find_all("a", href=True)
        
        print(f"      🔍 Processing {len(anchor_tags)} anchor tags")
        
        for a in anchor_tags:
            href = a.get("href", "")
            url = canonical_url(base_url, href)
            if url:
                links.add(url)
        
        print(f"      ✅ Extracted {len(links)} valid URLs")
        return links
    except Exception as e:
        print(f"      ❌ Error extracting links: {e}")
        return set()

def _looks_like_product_url(url, keywords):
    path = urlparse(url).path.lower()
    return any(k in path for k in keywords)

async def fetch(client, url, timeout):
    """Fetch using ScrapingBee API"""
    try:
        api_key = os.getenv('SCRAPINGBEE_API_KEY')
        if not api_key:
            return None
            
        sb_client = ScrapingBeeClient(api_key=api_key)
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: sb_client.get(url, params={
                'render_js': 'false',
                'premium_proxy': 'true',
                'country_code': 'us'
            })
        )
        
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print(f"ScrapingBee fetch error for {url}: {e}")
        return None
    return None

async def crawl(base_url, allowed_domains, product_url_keywords, concurrency=6, timeout=30, max_pages=10000):
    seen = set()
    to_visit = asyncio.Queue()
    await to_visit.put(base_url)
    product_candidates = set()

    async with httpx.AsyncClient(headers=DEFAULT_HEADERS) as client:
        sem = asyncio.Semaphore(concurrency)

        async def worker():
            while True:
                try:
                    url = await to_visit.get()
                except asyncio.CancelledError:
                    return
                if url in seen:
                    to_visit.task_done()
                    continue
                seen.add(url)

                if not is_same_domain(url, allowed_domains):
                    to_visit.task_done()
                    continue

                async with sem:
                    html = await fetch(client, url, timeout)
                if html:
                    for link in _extract_links(url, html):
                        if link not in seen and is_same_domain(link, allowed_domains):
                            if _looks_like_product_url(link, product_url_keywords):
                                product_candidates.add(link)
                            await to_visit.put(link)

                to_visit.task_done()
                if len(seen) >= max_pages:
                    break

        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await to_visit.join()
        for t in tasks:
            t.cancel()
        return list(product_candidates)

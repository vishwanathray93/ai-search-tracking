# -*- coding: utf-8 -*-
import sys
import os
sys.stdout.reconfigure(encoding='utf-8')

import requests
import psycopg2
import psycopg2.extras
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load .env file
load_dotenv()

# ==========================
# CONFIG
# ==========================
BC_STORE_HASH   = os.getenv("BC_STORE_HASH")
BC_CLIENT_ID    = os.getenv("BC_CLIENT_ID")
BC_ACCESS_TOKEN = os.getenv("BC_ACCESS_TOKEN")

BC_API_BASE = f"https://api.bigcommerce.com/stores/{BC_STORE_HASH}/v3"
BC_HEADERS  = {
    "X-Auth-Token": BC_ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept":       "application/json"
}

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
INDEX          = "products"

# ==========================
# REQUESTS SESSION WITH RETRY
# SSL errors aur network issues handle karta hai
# ==========================
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,                        # 5 baar retry karo
        backoff_factor=2,               # 2s, 4s, 8s, 16s, 32s wait
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = get_session()

# ==========================
# DB CONNECTION
# ==========================
def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "bc_products_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Password!23")
    )

# ==========================
# FETCH TRENDING SCORES FROM DB
# ==========================
def get_trending_scores():
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT product_id, trending_score, views, clicks, add_to_cart, orders 
        FROM product_metrics
    """)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    scores = {}
    for row in rows:
        scores[str(row["product_id"])] = {
            "trending_score": float(row["trending_score"] or 0),
            "views":          int(row["views"] or 0),
            "clicks":         int(row["clicks"] or 0),
            "add_to_cart":    int(row["add_to_cart"] or 0),
            "orders":         int(row["orders"] or 0)
        }
    return scores

# ==========================
# FETCH ALL PRODUCTS FROM BC
# Auto retry on SSL error + delay between pages
# ==========================
def fetch_bc_products(start_page=1, max_pages=None):
    all_products = []
    page = start_page

    print(f"[{datetime.now()}] Fetching products from BigCommerce...")

    while True:
        url    = f"{BC_API_BASE}/catalog/products"
        params = {
            "page":    page,
            "limit":   250,
            "include": "images,custom_fields"
        }

        # Retry loop for SSL errors
        success = False
        for attempt in range(5):
            try:
                res = SESSION.get(url, headers=BC_HEADERS, params=params, timeout=30)
                success = True
                break
            except Exception as e:
                wait = (attempt + 1) * 3
                print(f"   Attempt {attempt+1} failed page {page}: {type(e).__name__}")
                print(f"   Retrying in {wait}s...")
                time.sleep(wait)

        if not success:
            print(f"   Failed page {page} after 5 attempts — stopping here")
            break

        if res.status_code != 200:
            print(f"   BC API Error {res.status_code} on page {page} — stopping")
            break

        data     = res.json()
        products = data.get("data", [])

        if not products:
            print(f"   No more products on page {page} — done")
            break

        all_products.extend(products)
        print(f"   Page {page} — {len(products)} fetched (total: {len(all_products)})")

        # Check pagination
        meta        = data.get("meta", {}).get("pagination", {})
        total_pages = meta.get("total_pages", 1)

        # Stop if max_pages reached
        if max_pages and page >= (start_page + max_pages - 1):
            print(f"   Reached max_pages limit ({max_pages})")
            break

        if page >= total_pages:
            print(f"   All pages fetched!")
            break

        page += 1

        # Small delay between pages to avoid rate limiting
        time.sleep(0.3)

    print(f"[{datetime.now()}] Total products fetched: {len(all_products)}")
    return all_products

# ==========================
# FETCH CATEGORIES FROM BC
# ==========================
def fetch_categories():
    try:
        url    = f"{BC_API_BASE}/catalog/categories"
        params = {"limit": 250}
        res    = SESSION.get(url, headers=BC_HEADERS, params=params, timeout=30)

        if res.status_code != 200:
            return {}

        categories = {}
        for cat in res.json().get("data", []):
            categories[cat["id"]] = cat["name"]
        return categories
    except Exception as e:
        print(f"   Warning: Could not fetch categories: {e}")
        return {}

# ==========================
# BUILD OPENSEARCH DOCUMENT
# ==========================
def build_doc(product, categories, trending_scores):
    pid = str(product["id"])

    # Primary image
    images    = product.get("images", [])
    image_url = ""
    for img in images:
        if img.get("is_thumbnail"):
            image_url = img.get("url_standard", "")
            break
    if not image_url and images:
        image_url = images[0].get("url_standard", "")

    # Category names
    cat_ids   = product.get("categories", [])
    cat_names = [categories.get(cid, "") for cid in cat_ids if cid in categories]

    # Price
    sale_price = product.get("sale_price") or product.get("calculated_price") or product.get("price") or 0

    # Trending scores
    score_data = trending_scores.get(pid, {
        "trending_score": 0.0,
        "views":          0,
        "clicks":         0,
        "add_to_cart":    0,
        "orders":         0
    })

    # URL
    custom_url = (product.get("custom_url") or {}).get("url") or f"/{pid}/"

    # Description — strip HTML tags roughly
    desc = product.get("description", "") or ""
    desc = desc[:300]

    return {
        "product_id":     pid,
        "name":           product.get("name", ""),
        "description":    desc,
        "price":          float(product.get("price") or 0),
        "sale_price":     float(sale_price),
        "category":       cat_names,
        "brand":          product.get("brand_name", "") or "",
        "image_url":      image_url,
        "url":            custom_url,
        "in_stock":       (
            product.get("availability", "") != "disabled" and
            product.get("inventory_level", 1) > 0
        ),
        "is_visible":     product.get("is_visible", True),
        "trending_score": score_data["trending_score"],
        "views":          score_data["views"],
        "clicks":         score_data["clicks"],
        "add_to_cart":    score_data["add_to_cart"],
        "orders":         score_data["orders"],
        "tags":           (
            product.get("search_keywords", "").split(",")
            if product.get("search_keywords") else []
        )
    }

# ==========================
# BULK INDEX TO OPENSEARCH
# ==========================
def bulk_index(docs):
    if not docs:
        return 0

    bulk_body = ""
    for doc in docs:
        bulk_body += json.dumps({
            "index": {"_index": INDEX, "_id": doc["product_id"]}
        }) + "\n"
        bulk_body += json.dumps(doc) + "\n"

    try:
        res = requests.post(
            f"{OPENSEARCH_URL}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk_body.encode("utf-8"),
            timeout=30
        )

        if res.status_code not in [200, 201]:
            print(f"   Bulk HTTP error: {res.status_code}")
            return 0

        result = res.json()

        # Show first error if any
        if result.get("errors"):
            for item in result.get("items", [])[:2]:
                err = item.get("index", {}).get("error")
                if err:
                    print(f"   Index error: {err.get('type')} - {err.get('reason', '')[:100]}")

        success = sum(
            1 for item in result.get("items", [])
            if item.get("index", {}).get("result") in ["created", "updated"]
        )
        return success

    except Exception as ex:
        print(f"   Bulk index exception: {ex}")
        return 0

# ==========================
# MAIN
# ==========================
def main():
    print("=" * 50)
    print("BC -> OpenSearch Product Import")
    print("=" * 50)
    print()

    # Validate credentials
    if not BC_STORE_HASH or not BC_ACCESS_TOKEN:
        print("Missing BC credentials -- check your .env file")
        print("  BC_STORE_HASH, BC_CLIENT_ID, BC_ACCESS_TOKEN required")
        return

    # Step 1: Trending scores
    print(f"[{datetime.now()}] Loading trending scores from DB...")
    trending_scores = get_trending_scores()
    print(f"   Found {len(trending_scores)} products with scores")

    # Step 2: Categories
    print(f"[{datetime.now()}] Fetching categories...")
    categories = fetch_categories()
    print(f"   Found {len(categories)} categories")

    # Step 3: Fetch products
    # max_pages=None means fetch ALL pages
    # max_pages=8    means fetch 8 x 250 = 2000 products only
    products = fetch_bc_products(start_page=483, max_pages=None)

    if not products:
        print("No products fetched -- check BC credentials")
        return

    # Step 4: Bulk index in batches
    print(f"\n[{datetime.now()}] Indexing to OpenSearch...")

    batch_size    = 250
    total_indexed = 0

    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        docs  = [build_doc(p, categories, trending_scores) for p in batch]
        docs  = [d for d in docs if d["is_visible"]]  # visible only

        count          = bulk_index(docs)
        total_indexed += count

        batch_num = i // batch_size + 1
        print(f"   Batch {batch_num} -- indexed {count}/{len(docs)} (total: {total_indexed})")

    print()
    print(f"[{datetime.now()}] Import complete -- {total_indexed} products indexed")
    print(f"Verify: http://localhost:9200/products/_count")
    print()

if __name__ == "__main__":
    main()

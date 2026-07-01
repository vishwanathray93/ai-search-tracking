# -*- coding: utf-8 -*-
"""
fetch_categories.py
-------------------
Pulls all unique categories from the OpenSearch `products` index
(via a terms aggregation) and enriches each with its BigCommerce
custom URL.

Requirements:
    pip install requests python-dotenv

Usage:
    python fetch_categories.py
    python fetch_categories.py --size 200     # fetch up to 200 categories
    python fetch_categories.py --csv          # also write categories.csv
"""

import os
import sys
import json
import argparse
import csv
from datetime import datetime
from dotenv import load_dotenv
import requests

load_dotenv()

# ─────────────────────────────────────────
# CONFIG  (reads from .env or environment)
# ─────────────────────────────────────────
OPENSEARCH_URL  = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
INDEX           = "products"

BC_STORE_HASH   = os.getenv("BC_STORE_HASH", "")
BC_ACCESS_TOKEN = os.getenv("BC_ACCESS_TOKEN", "")
BC_API_BASE     = f"https://api.bigcommerce.com/stores/{BC_STORE_HASH}/v3"
BC_HEADERS      = {
    "X-Auth-Token": BC_ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept":       "application/json",
}

BC_STORE_DOMAIN = os.getenv("BC_STORE_DOMAIN", "")   # e.g. "https://www.mystore.com"


# ─────────────────────────────────────────
# STEP 1 — Aggregate unique categories from OpenSearch
# ─────────────────────────────────────────
def fetch_os_categories(size: int = 200) -> list[dict]:
    """
    Returns a list of dicts:
        {"name": "Men's Shoes", "product_count": 142}
    sorted by product_count DESC.
    """
    query = {
        "size": 0,           # we only want agg results, not hits
        "aggs": {
            "all_categories": {
                "terms": {
                    "field": "category.keyword",   # needs .keyword sub-field
                    "size":  size,
                    "order": {"_count": "desc"}
                }
            }
        }
    }

    url = f"{OPENSEARCH_URL}/{INDEX}/_search"
    try:
        res = requests.post(url, json=query, timeout=30)
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Cannot reach OpenSearch at {OPENSEARCH_URL}")
        print("        Make sure OpenSearch is running and OPENSEARCH_URL is set.")
        sys.exit(1)

    if res.status_code != 200:
        # Fallback: try without .keyword (for text fields with fielddata enabled)
        query["aggs"]["all_categories"]["terms"]["field"] = "category"
        res2 = requests.post(url, json=query, timeout=30)
        if res2.status_code != 200:
            print(f"[ERROR] OpenSearch aggregation failed: {res.status_code}")
            print(res.text[:500])
            sys.exit(1)
        res = res2

    buckets = (
        res.json()
           .get("aggregations", {})
           .get("all_categories", {})
           .get("buckets", [])
    )
    return [
        {"name": b["key"], "product_count": b["doc_count"]}
        for b in buckets
    ]


# ─────────────────────────────────────────
# STEP 2 — Fetch category URLs from BigCommerce
# ─────────────────────────────────────────
def fetch_bc_category_urls() -> dict:
    """
    Returns {category_name: full_url, ...}
    Fetches ALL pages from BC v3 categories endpoint.
    """
    if not BC_STORE_HASH or not BC_ACCESS_TOKEN:
        print("[WARN] BC credentials not set — URLs will be empty.")
        print("       Set BC_STORE_HASH, BC_ACCESS_TOKEN (and optionally BC_STORE_DOMAIN) in .env")
        return {}

    url_map = {}
    page    = 1

    print(f"[{datetime.now():%H:%M:%S}] Fetching BC categories...")

    while True:
        params = {"limit": 250, "page": page, "include": ""}
        try:
            res = requests.get(
                f"{BC_API_BASE}/catalog/categories",
                headers=BC_HEADERS,
                params=params,
                timeout=30,
            )
        except Exception as e:
            print(f"[WARN] BC request failed page {page}: {e}")
            break

        if res.status_code != 200:
            print(f"[WARN] BC API returned {res.status_code} on page {page}")
            break

        data = res.json()
        cats = data.get("data", [])

        if not cats:
            break

        for cat in cats:
            name       = cat.get("name", "")
            custom_url = (cat.get("custom_url") or {}).get("url", "")
            if BC_STORE_DOMAIN and custom_url:
                full_url = BC_STORE_DOMAIN.rstrip("/") + custom_url
            elif custom_url:
                full_url = custom_url          # relative path
            else:
                slug     = name.lower().replace(" ", "-").replace("'", "")
                full_url = f"/{slug}/"         # best-guess fallback

            url_map[name] = full_url

        meta       = data.get("meta", {}).get("pagination", {})
        total_pages = meta.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1

    print(f"           → {len(url_map)} BC categories loaded")
    return url_map


# ─────────────────────────────────────────
# STEP 3 — Merge & Display
# ─────────────────────────────────────────
def display_table(rows: list[dict]) -> None:
    col_w = {"#": 5, "Category": 50, "Products": 10, "URL": 60}
    sep   = "+" + "+".join("-" * (w + 2) for w in col_w.values()) + "+"
    hdr   = (
        f"| {'#':>{col_w['#']}} "
        f"| {'Category':<{col_w['Category']}} "
        f"| {'Products':>{col_w['Products']}} "
        f"| {'URL':<{col_w['URL']}} |"
    )

    print()
    print(sep)
    print(hdr)
    print(sep)

    for i, row in enumerate(rows, 1):
        name  = row["name"][:col_w["Category"]]
        url   = row["url"][:col_w["URL"]]
        count = row["product_count"]
        print(
            f"| {i:>{col_w['#']}} "
            f"| {name:<{col_w['Category']}} "
            f"| {count:>{col_w['Products']}} "
            f"| {url:<{col_w['URL']}} |"
        )

    print(sep)
    print(f"\n  Total categories shown: {len(rows)}")


def write_csv(rows: list[dict], path: str = "categories.csv") -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["#", "name", "product_count", "url"])
        writer.writeheader()
        for i, row in enumerate(rows, 1):
            writer.writerow({"#": i, **row})
    print(f"\n  CSV saved → {path}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fetch categories from OpenSearch + BC URLs")
    parser.add_argument("--size", type=int, default=200, help="Max categories to fetch (default 200)")
    parser.add_argument("--csv",  action="store_true",   help="Write results to categories.csv")
    args = parser.parse_args()

    print("=" * 60)
    print("  OpenSearch Category Viewer")
    print(f"  OpenSearch: {OPENSEARCH_URL}/{INDEX}")
    print("=" * 60)

    # 1. OpenSearch aggregation
    print(f"[{datetime.now():%H:%M:%S}] Aggregating categories from OpenSearch (size={args.size})...")
    os_cats = fetch_os_categories(args.size)
    print(f"           → {len(os_cats)} unique categories found in index")

    if not os_cats:
        print("\n[ERROR] No categories returned.")
        print("  Possible reasons:")
        print("  • The 'category' field has no .keyword sub-field → add a mapping or enable fielddata")
        print("  • The index is empty (run bc_import.py first)")
        sys.exit(1)

    # 2. BC category URLs
    bc_urls = fetch_bc_category_urls()

    # 3. Merge
    rows = []
    for cat in os_cats:
        name = cat["name"]
        rows.append({
            "name":          name,
            "product_count": cat["product_count"],
            "url":           bc_urls.get(name, f"/{name.lower().replace(' ', '-')}/"),
        })

    # 4. Display
    display_table(rows)

    # 5. Optional CSV
    if args.csv:
        write_csv(rows)

    # Summary of missing URLs
    missing = [r for r in rows if not bc_urls.get(r["name"])]
    if missing:
        print(f"\n  [NOTE] {len(missing)} categories had no exact BC URL match (fallback slug used).")
        print("         Set BC_STORE_DOMAIN in .env for absolute URLs.\n")


if __name__ == "__main__":
    main()

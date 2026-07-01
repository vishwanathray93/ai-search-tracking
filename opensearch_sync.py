import psycopg2
import requests
import json
import os
from datetime import datetime

OPENSEARCH_URL = "http://localhost:9200"
INDEX = "products"

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "bc_products_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Password!23")
    )

def sync_trending_scores():
    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        SELECT product_id, views, clicks, add_to_cart, orders, 
               wishlist, trending_score
        FROM product_metrics
        ORDER BY trending_score DESC
    """)

    rows = cursor.fetchall()
    cursor.close()
    db.close()

    if not rows:
        print("No metrics to sync")
        return

    # Bulk update OpenSearch
    bulk_body = ""
    for row in rows:
        product_id, views, clicks, atc, orders, wishlist, score = row
        bulk_body += json.dumps({
            "update": {"_index": INDEX, "_id": product_id}
        }) + "\n"
        bulk_body += json.dumps({
            "doc": {
                "trending_score": score,
                "views": views,
                "clicks": clicks,
                "add_to_cart": atc,
                "orders": orders,
                "wishlist": wishlist
            },
            "doc_as_upsert": True
        }) + "\n"

    res = requests.post(
        f"{OPENSEARCH_URL}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=bulk_body
    )

    print(f"[{datetime.now()}] ✅ Synced {len(rows)} products to OpenSearch")

if __name__ == "__main__":
    sync_trending_scores()
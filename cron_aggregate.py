import psycopg2
import os
from datetime import datetime

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "bc_products_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Password!23")
    )

def aggregate_metrics():
    db = get_db()
    cursor = db.cursor()

    try:
        # -------------------------------------------------------
        # FIX 1: skip_filter replaced with a safe parameterized
        # approach using a VALUES list — avoids SQL injection risk
        # and is cleaner than f-string formatting into SQL.
        # -------------------------------------------------------
        skip_slugs = (
            'search.php', 'cart.php', 'checkout',
            'account', 'login', 'order-confirmation'
        )

        # -------------------------------------------------------
        # FIX 2: Removed the two Python `#` inline comments that
        # were sitting INSIDE the SQL string on the click and
        # wishlist lines. PostgreSQL does not recognise `#` as a
        # comment character — those lines caused a SQL syntax
        # error every single time the cron ran, so NO metrics
        # were ever being aggregated.
        #
        # FIX 3: Added `wishlist` column to both the INSERT
        # column list and the SELECT calculation.  The
        # product_metrics table has a `wishlist` column but the
        # original query never populated it, so opensearch_sync
        # was always syncing 0 for every product's wishlist count.
        #
        # FIX 4: Added `wishlist` to the ON CONFLICT UPDATE block
        # so that subsequent runs also keep the wishlist count
        # up-to-date instead of leaving it stale.
        # -------------------------------------------------------
        cursor.execute("""
            INSERT INTO product_metrics
                (product_id, views, impressions, clicks,
                 add_to_cart, wishlist, orders,
                 trending_score, updated_at)
            SELECT
                product_id,
                COUNT(*) FILTER (WHERE event_type = 'view')        AS views,
                COUNT(*) FILTER (WHERE event_type = 'impression')  AS impressions,
                COUNT(*) FILTER (WHERE event_type = 'click')       AS clicks,
                COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart,
                COUNT(*) FILTER (WHERE event_type = 'wishlist')    AS wishlist,
                COUNT(*) FILTER (WHERE event_type = 'purchase')    AS orders,
                (
                    COUNT(*) FILTER (WHERE event_type = 'view')        * 0.4  +
                    COUNT(*) FILTER (WHERE event_type = 'click')       * 0.3  +
                    COUNT(*) FILTER (WHERE event_type = 'wishlist')    * 0.25 +
                    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') * 0.35 +
                    COUNT(*) FILTER (WHERE event_type = 'purchase')    * 2.0
                ) AS trending_score,
                NOW()
            FROM events
            WHERE product_id IS NOT NULL
              AND product_id != ''
              AND product_id NOT IN %s
            GROUP BY product_id

            ON CONFLICT (product_id) DO UPDATE SET
                views          = EXCLUDED.views,
                impressions    = EXCLUDED.impressions,
                clicks         = EXCLUDED.clicks,
                add_to_cart    = EXCLUDED.add_to_cart,
                wishlist       = EXCLUDED.wishlist,
                orders         = EXCLUDED.orders,
                trending_score = EXCLUDED.trending_score,
                updated_at     = NOW()
        """, (skip_slugs,))

        db.commit()
        print(f"[{datetime.now()}] Aggregation complete")

    except Exception as ex:
        db.rollback()
        print(f"[{datetime.now()}] Aggregation failed: {ex}")

    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    aggregate_metrics()

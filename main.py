# main.py
# -*- coding: utf-8 -*-

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
import psycopg2.extras
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# ==========================
# CORS
# ==========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ==========================
# CONFIG
# ==========================
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
INDEX          = "products"

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
# HELPER FUNCTIONS
# ==========================
def fetch_products_by_ids(product_ids: list, limit: int = 20) -> list:
    if not product_ids:
        return []

    numeric_ids = [pid for pid in product_ids if str(pid).isdigit()]
    slug_ids    = [pid for pid in product_ids if not str(pid).isdigit()]

    should_clauses = []

    if numeric_ids:
        should_clauses.append({
            "terms": {"product_id": [str(pid) for pid in numeric_ids]}
        })

    for slug in slug_ids:
        should_clauses.append({
            "term": {"url": f"/{slug}/"}
        })

    if not should_clauses:
        return []

    query = {
        "query": {
            "bool": {
                "should":               should_clauses,
                "minimum_should_match": 1
            }
        },
        "size": limit
    }

    try:
        res = requests.post(
            f"{OPENSEARCH_URL}/{INDEX}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
            timeout=5
        )
        if res.status_code != 200:
            return []

        hits = res.json().get("hits", {}).get("hits", [])

        # Preserve original ordering
        id_order = {str(pid): i for i, pid in enumerate(product_ids)}
        docs = [h["_source"] for h in hits if h.get("_source")]
        docs.sort(key=lambda d: id_order.get(str(d.get("product_id", "")), 999))
        return docs

    except Exception as ex:
        print(f"[OpenSearch] fetch_products_by_ids error: {ex}")
        return []


def search_opensearch(query_body: dict) -> list:
    try:
        res = requests.post(
            f"{OPENSEARCH_URL}/{INDEX}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(query_body),
            timeout=5
        )
        if res.status_code != 200:
            return []
        hits = res.json().get("hits", {}).get("hits", [])
        return [h["_source"] for h in hits if h.get("_source")]
    except Exception as ex:
        print(f"[OpenSearch] search_opensearch error: {ex}")
        return []


# ==========================
# MODELS
# ==========================
class Event(BaseModel):
    event_type: str
    session_id: str
    product_id: Optional[str] = None
    user_id:    Optional[str] = None
    visitor_id: Optional[str] = None
    query:      Optional[str] = None
    position:   Optional[int] = None
    source:     Optional[str] = None
    value:      Optional[float] = None
    order_id:   Optional[str] = None
    timestamp:  Optional[str] = None
    result_count: Optional[int] = None   # number of results a search returned
    response_ms:  Optional[int] = None   # how long the search took (ms)
    word_count:   Optional[int] = None   # words in the search query (billing unit)

class EventBatch(BaseModel):
    events: List[Event]


# ==========================
# /track
# ==========================
@app.post("/track")
def track_events(batch: EventBatch):
    if not batch.events:
        raise HTTPException(status_code=400, detail="No events provided")

    db     = get_db()
    cursor = db.cursor()

    try:
        # Compute word_count server-side from each event's query text so billing
        # never depends on the storefront tracker sending it. Non-search events
        # have no query -> 0. Matches the SQL backfill in the migration exactly
        # (whitespace-split, trimmed).
        rows = []
        for e in batch.events:
            d = e.dict()
            q = (d.get("query") or "").strip()
            d["word_count"] = len(q.split()) if q else 0
            rows.append(d)
        psycopg2.extras.execute_batch(cursor, """
    INSERT INTO events
        (event_type, product_id, user_id, session_id, visitor_id, query, position, value, source, result_count, response_ms, word_count)
    VALUES
        (%(event_type)s, %(product_id)s, %(user_id)s, %(session_id)s, %(visitor_id)s,
         %(query)s, %(position)s, %(value)s, %(source)s, %(result_count)s, %(response_ms)s, %(word_count)s)
""", rows)

        db.commit()
        return {"status": "ok", "saved": len(batch.events)}

    except Exception as ex:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(ex))

    finally:
        cursor.close()
        db.close()


# ==========================
# /trending — ai-trending.js
# Modified to use custom formula: (views × 0.4 + add_to_cart × 0.35 + purchases × 2.0)
# ==========================
@app.get("/trending")
def get_trending(
    visitor_id: Optional[str] = None,
    user_id:    Optional[str] = None,
    limit:      int = 10
):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term":  {"is_visible": True}},
                    {"term":  {"in_stock":   True}}
                ]
            }
        },
        "sort": [
            {
                "_script": {
                    "type":  "number",
                    "order": "desc",
                    "script": {
                        "source": """
                            double v = doc.containsKey('views') ? doc['views'].value : 0.0;
                            double a = doc.containsKey('add_to_cart') ? doc['add_to_cart'].value : 0.0;
                            double p = doc.containsKey('orders') ? doc['orders'].value : 0.0;
                            return (v * 0.4) + (a * 0.35) + (p * 2.0);
                        """
                    }
                }
            }
        ],
        "size": limit
    }
    return {"results": search_opensearch(query)}


# ==========================
# /recommendations — ai-keep-shoping.js
#
# Logic:
#   1. User has view/click history
#      → Return EXACTLY those products, most-recent first (sliding window).
#        Each new view/click pushes the oldest product out of the visible
#        window so the list updates one-by-one as the user browses.
#      → Products the user added to cart OR wishlist are excluded here;
#        they appear in /pick-up instead.
#   2. First-time / no-history visitor
#      → Fallback: random visible + in-stock products.
# ==========================
@app.get("/recommendations")
def get_recommendations(
    visitor_id: Optional[str] = None,
    user_id:    Optional[str] = None,
    limit:      int = 8
):
    db     = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    viewed_ids = []

    try:
        if visitor_id or user_id:
            filters = []
            params  = []

            if visitor_id:
                filters.append("visitor_id = %s")
                params.append(visitor_id)
            if user_id:
                filters.append("user_id = %s")
                params.append(user_id)

            where = " OR ".join(filters)

            # Fetch products viewed/clicked by this user, ordered most-recent
            # first (sliding window). Exclude anything they added to cart or
            # wishlist — those belong in /pick-up.
            # params * 2: the WHERE clause with ({where}) appears twice.
            cursor.execute(f"""
                SELECT product_id, MAX(created_at) AS last_seen
                FROM events
                WHERE ({where})
                  AND event_type IN ('view', 'click')
                  AND product_id IS NOT NULL
                  AND product_id != ''
                  AND product_id NOT IN (
                      SELECT DISTINCT product_id
                      FROM events
                      WHERE ({where})
                        AND event_type IN ('add_to_cart', 'add_to_wishlist')
                        AND product_id IS NOT NULL
                  )
                GROUP BY product_id
                ORDER BY last_seen DESC
                LIMIT %s
            """, (*params * 2, limit))

            viewed_ids = [row["product_id"] for row in cursor.fetchall()]

    except Exception as ex:
        print(f"[/recommendations] DB error: {ex}")
    finally:
        cursor.close()
        db.close()

    # ── Case 1: user has viewed/clicked products ──────────────────────────
    # Viewed/clicked products fill slots from the TOP one by one.
    # Remaining slots are filled with fallback (random) products.
    # Example with limit=8:
    #   1 view  → [viewed_1, fallback, fallback, fallback, fallback, fallback, fallback, fallback]
    #   2 views → [viewed_1, viewed_2, fallback, fallback, fallback, fallback, fallback, fallback]
    #   8 views → [viewed_1, ..., viewed_8]  (all slots filled, no fallback needed)
    if viewed_ids:
        viewed_docs = fetch_products_by_ids(viewed_ids, limit=limit)
        # Safety filter: only surface products that are visible and in stock
        viewed_docs = [d for d in viewed_docs if d.get("is_visible", True) and d.get("in_stock", True)]

        remaining = limit - len(viewed_docs)

        if remaining <= 0:
            # Viewed products fill all slots
            return {"results": viewed_docs[:limit]}

        # Fill remaining slots with random products, excluding already shown ones
        viewed_product_ids = [str(d.get("product_id", "")) for d in viewed_docs]
        fallback_query = {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"is_visible": True}},
                                {"term": {"in_stock":   True}}
                            ],
                            "must_not": [
                                {"terms": {"product_id": viewed_product_ids}}
                            ]
                        }
                    },
                    "random_score": {}
                }
            },
            "size": remaining
        }
        fallback_docs = search_opensearch(fallback_query)
        return {"results": viewed_docs + fallback_docs}

    # ── Case 2: no history — first-time visitor ───────────────────────────
    # Return random visible + in-stock products as a neutral starting point.
    fallback_query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"is_visible": True}},
                            {"term": {"in_stock":   True}}
                        ]
                    }
                },
                "random_score": {}
            }
        },
        "size": limit
    }
    return {"results": search_opensearch(fallback_query)}


# ==========================
# /pick-up — ai-pickup.js
# Returns exact cart/wishlist items.
# Fallback is random 4 products (can pass exclude_ids from frontend to avoid match).
# ==========================
@app.get("/pick-up")
def get_pick_up(
    visitor_id:  Optional[str] = None,
    user_id:     Optional[str] = None,
    exclude_ids: Optional[str] = None, # Comma separated IDs passed by frontend
    limit:       int = 8
):
    if not visitor_id and not user_id:
        return {"results": []}

    db     = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    product_ids = []

    try:
        filters = []
        params  = []

        if visitor_id:
            filters.append("visitor_id = %s")
            params.append(visitor_id)
        if user_id:
            filters.append("user_id = %s")
            params.append(user_id)

        where = " OR ".join(filters)

        # Get items added to cart
        cursor.execute(f"""
            SELECT product_id, MAX(created_at) AS last_action
            FROM events
            WHERE ({where})
              AND event_type = 'add_to_cart'
              AND product_id IS NOT NULL
              AND product_id != ''
            GROUP BY product_id
            ORDER BY last_action DESC
            LIMIT %s
        """, (*params, limit))
        cart_ids = [row["product_id"] for row in cursor.fetchall()]

        # Get items added to wishlist
        wishlist_ids = []
        if visitor_id:
            try:
                # Also check events table just in case wishlist is an event_type
                cursor.execute(f"""
                    SELECT product_id, MAX(created_at) AS last_action
                    FROM events
                    WHERE ({where})
                      AND event_type = 'add_to_wishlist'
                      AND product_id IS NOT NULL
                    GROUP BY product_id
                    ORDER BY last_action DESC
                    LIMIT %s
                """, (*params, limit))
                wishlist_ids = [row["product_id"] for row in cursor.fetchall()]
            except Exception:
                pass

        # Merge cart + wishlist, de-duplicate
        seen = set()
        for pid in cart_ids + wishlist_ids:
            if pid not in seen:
                product_ids.append(pid)
                seen.add(pid)

    except Exception as ex:
        print(f"[/pick-up] DB error: {ex}")
    finally:
        cursor.close()
        db.close()

    # If exact cart/wishlist matches exist, return them
    if product_ids:
        docs = fetch_products_by_ids(product_ids[:limit], limit=limit)
        docs = [d for d in docs if d.get("is_visible", True) and d.get("in_stock", True)]
        return {"results": docs}

    # FALLBACK: 4 Random On-Sale Products that don't match other sections
    must_not_clauses = []
    if exclude_ids:
        ids_to_exclude = [x.strip() for x in exclude_ids.split(",")]
        must_not_clauses.append({"terms": {"product_id": ids_to_exclude}})
        
    random_query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"is_visible": True}},
                            {"term": {"in_stock": True}}
                        ],
                        "must_not": must_not_clauses
                    }
                },
                "random_score": {}
            }
        },
        "size": 4  # Strict limit of 4 per requirement
    }
    
    return {"results": search_opensearch(random_query)}


# ==========================
# /recently-viewed
# ==========================
@app.get("/recently-viewed/{visitor_id}")
def get_recently_viewed(visitor_id: str, limit: int = 6):
    db     = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT product_id, MAX(created_at) AS last_viewed
        FROM events
        WHERE visitor_id = %s
          AND event_type = 'view'
          AND product_id IS NOT NULL
        GROUP BY product_id
        ORDER BY last_viewed DESC
        LIMIT %s
    """, (visitor_id, limit))
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    product_ids = [row["product_id"] for row in rows]
    docs        = fetch_products_by_ids(product_ids, limit=limit)
    return {"results": docs}


# ==========================
# /super-deals
# ==========================
@app.get("/super-deals")
def get_super_deals(limit: int = 10):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term":  {"is_visible": True}},
                    {"term":  {"in_stock":   True}},
                    {"range": {"add_to_cart": {"gt": 0}}}
                ]
            }
        },
        "sort": [
            {
                "_script": {
                    "type":  "number",
                    "order": "desc",
                    "script": {
                        "source": "doc['add_to_cart'].value * 0.35 + doc['orders'].value * 2.0"
                    }
                }
            }
        ],
        "size": limit
    }
    return {"results": search_opensearch(query)}


# ==========================
# /health
# ==========================
@app.get("/health")
def health():
    db_ok = False
    try:
        db = get_db()
        db.close()
        db_ok = True
    except Exception:
        pass

    os_ok = False
    try:
        res = requests.get(f"{OPENSEARCH_URL}/_cluster/health", timeout=3)
        os_ok = res.status_code == 200
    except Exception:
        pass

    return {
        "status":     "running",
        "database":   "ok" if db_ok else "error",
        "opensearch": "ok" if os_ok else "error",
        "timestamp":  datetime.now().isoformat()
    }


# ==========================
# ANALYTICS  (live, computed from the flat `events` table + OpenSearch)
# --------------------------------------------------------------------------
# Replaces the earlier inline /analytics/* endpoints. Key fixes:
#   * revenue & orders come from `events` purchases (sum(value) / distinct
#     sessions), NOT the `orders` table that /track never fills.
#   * overview adds previous-window deltas + CTR + conversion.
#   * funnel / top-searches return the CTR / conversion / revenue the UI needs.
#   * search-trend is gap-filled so the chart has a continuous daily line.
#   * avg_results / avg_response_ms are null (events tracks neither).
#   * every windowed endpoint takes ?days= so the 7/30/90 toggle re-scopes.
# Response envelope kept as { "success": true, "data": ... }.
# ==========================
from fastapi import Query


def _pct(cur, prev):
    """Percentage change vs previous window.
    - prev > 0  -> signed % (number)
    - prev == 0 but cur > 0 -> "new" (activity with no comparable prior window)
    - prev == 0 and cur == 0 -> None (renders as a dash)
    """
    if prev:
        return round((cur - prev) / prev * 100, 1)
    return "new" if (cur or 0) > 0 else None


def _rate(num, den):
    return round(num / den * 100, 1) if den else 0


# ==========================
# /analytics/overview
# ==========================
@app.get("/analytics/overview")
def analytics_overview(days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            WITH cur AS (SELECT * FROM events WHERE created_at >= now() - make_interval(days => %s)),
                 prv AS (SELECT * FROM events WHERE created_at >= now() - make_interval(days => %s)
                                                 AND created_at <  now() - make_interval(days => %s)),
                 -- one row per search "group": a search and the events that follow it
                 -- in the same session (until the next search). grp>=1 => after a search.
                 cur_g AS (
                   SELECT session_id, grp,
                          bool_or(event_type='click')    AS hc,
                          bool_or(event_type='purchase')  AS hp
                     FROM (SELECT session_id, event_type,
                                  count(*) FILTER (WHERE event_type='search')
                                    OVER (PARTITION BY session_id ORDER BY created_at
                                          ROWS UNBOUNDED PRECEDING) AS grp
                             FROM cur) z
                    WHERE grp >= 1 GROUP BY session_id, grp),
                 prv_g AS (
                   SELECT session_id, grp,
                          bool_or(event_type='click')    AS hc,
                          bool_or(event_type='purchase')  AS hp
                     FROM (SELECT session_id, event_type,
                                  count(*) FILTER (WHERE event_type='search')
                                    OVER (PARTITION BY session_id ORDER BY created_at
                                          ROWS UNBOUNDED PRECEDING) AS grp
                             FROM prv) z
                    WHERE grp >= 1 GROUP BY session_id, grp)
            SELECT
              (SELECT count(*) FROM cur WHERE event_type='search')                       AS searches,
              (SELECT count(DISTINCT session_id) FROM cur WHERE event_type='search')     AS sessions,
              (SELECT count(*) FILTER (WHERE hc) FROM cur_g)                             AS click_groups,
              (SELECT count(*) FILTER (WHERE hp) FROM cur_g)                             AS order_groups,
              (SELECT COALESCE(sum(value),0) FROM cur WHERE event_type='purchase')       AS revenue,
              (SELECT avg(result_count) FROM cur WHERE event_type='search' AND result_count IS NOT NULL) AS avg_results,
              (SELECT max(result_count) FROM cur WHERE event_type='search')              AS max_results,
              (SELECT avg(response_ms)  FROM cur WHERE event_type='search' AND response_ms  IS NOT NULL) AS avg_response_ms,
              (SELECT count(*) FROM prv WHERE event_type='search')                       AS p_searches,
              (SELECT count(DISTINCT session_id) FROM prv WHERE event_type='search')     AS p_sessions,
              (SELECT count(*) FILTER (WHERE hc) FROM prv_g)                             AS p_click_groups,
              (SELECT count(*) FILTER (WHERE hp) FROM prv_g)                             AS p_order_groups,
              (SELECT COALESCE(sum(value),0) FROM prv WHERE event_type='purchase')       AS p_revenue,
              (SELECT avg(response_ms)  FROM prv WHERE event_type='search' AND response_ms  IS NOT NULL) AS p_avg_response_ms
            """,
            (days, days * 2, days),
        )
        r = cursor.fetchone()
        searches   = r["searches"] or 0
        p_searches = r["p_searches"] or 0
        # CTR / conversion are now "share of searches that led to a click / order",
        # so both are bounded 0-100%.
        ctr  = _rate(r["click_groups"] or 0, searches)
        conv = _rate(r["order_groups"] or 0, searches)
        has_prev = p_searches > 0
        p_ctr  = _rate(r["p_click_groups"] or 0, p_searches)
        p_conv = _rate(r["p_order_groups"] or 0, p_searches)
        avg_resp   = round(float(r["avg_response_ms"]))   if r["avg_response_ms"]   is not None else None
        p_avg_resp = round(float(r["p_avg_response_ms"])) if r["p_avg_response_ms"] is not None else None
        resp_delta = (avg_resp - p_avg_resp) if (avg_resp is not None and p_avg_resp is not None) else None
        return {
            "success": True,
            "data": {
                "searches": searches,
                "sessions": r["sessions"] or 0,
                "orders": r["order_groups"] or 0,
                "revenue": round(float(r["revenue"] or 0), 2),
                "ctr": ctr,
                "conversion": conv,
                "avg_results": round(float(r["avg_results"]), 1) if r["avg_results"] is not None else None,
                "max_results": int(r["max_results"]) if r["max_results"] is not None else None,
                "avg_response_ms": avg_resp,
                "response_delta_ms": resp_delta,
                "searches_delta_pct": _pct(searches, p_searches),
                "sessions_delta_pct": _pct(r["sessions"] or 0, r["p_sessions"] or 0),
                "revenue_delta_pct": _pct(float(r["revenue"] or 0), float(r["p_revenue"] or 0)),
                "ctr_delta_pt": round(ctr - p_ctr, 1) if has_prev else None,
                "conversion_delta_pt": round(conv - p_conv, 1) if has_prev else None,
            },
        }
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/search-trend  (gap-filled daily series)
# ==========================
@app.get("/analytics/search-trend")
def analytics_search_trend(days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT d::date AS date,
              (SELECT count(*) FROM events e WHERE e.event_type='search'
                 AND e.created_at>=d AND e.created_at<d+interval '1 day')                 AS searches,
              (SELECT count(*) FROM events e WHERE e.event_type='click'
                 AND e.created_at>=d AND e.created_at<d+interval '1 day')                 AS clicks,
              (SELECT count(DISTINCT session_id) FROM events e WHERE e.event_type='purchase'
                 AND e.created_at>=d AND e.created_at<d+interval '1 day')                 AS conversions
            FROM generate_series((now() - make_interval(days => %s - 1))::date, now()::date, interval '1 day') d
            ORDER BY d
            """,
            (days,),
        )
        data = [
            {
                "date": str(row["date"]),
                "searches": row["searches"] or 0,
                "clicks": row["clicks"] or 0,
                "orders": row["conversions"] or 0,
                "conversions": row["conversions"] or 0,
            }
            for row in cursor.fetchall()
        ]
        return {"success": True, "data": data}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/conversion-funnel
# ==========================
@app.get("/analytics/conversion-funnel")
def analytics_conversion_funnel(days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            WITH ev AS (
              SELECT session_id, event_type, value,
                     count(*) FILTER (WHERE event_type='search')
                       OVER (PARTITION BY session_id ORDER BY created_at
                             ROWS UNBOUNDED PRECEDING) AS grp
                FROM events WHERE created_at >= now() - make_interval(days => %s)
            ),
            g AS (SELECT session_id, grp,
                         bool_or(event_type='click')    AS hc,
                         bool_or(event_type='purchase')  AS hp,
                         sum(value) FILTER (WHERE event_type='purchase') AS rev
                    FROM ev WHERE grp >= 1 GROUP BY session_id, grp)
            SELECT
              (SELECT count(*) FROM events
                 WHERE event_type='search'
                   AND created_at >= now() - make_interval(days => %s))  AS searches,
              count(*) FILTER (WHERE hc)        AS clicks,
              count(*) FILTER (WHERE hp)        AS orders,
              COALESCE(sum(rev),0)              AS revenue
              FROM g
            """,
            (days, days),
        )
        f = cursor.fetchone()
        s = f["searches"] or 0
        return {
            "success": True,
            "data": {
                # searches -> searches-that-got-a-click -> searches-that-converted,
                # so the funnel strictly narrows and the rates stay 0-100%.
                "searches": s,
                "clicks": f["clicks"] or 0,
                "orders": f["orders"] or 0,
                "revenue": round(float(f["revenue"] or 0), 2),
                "ctr_pct": _rate(f["clicks"] or 0, s),
                "conversion_pct": _rate(f["orders"] or 0, s),
            },
        }
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/top-searches  (per-query CTR / conversion / revenue via last-touch attribution)
# ==========================
@app.get("/analytics/top-searches")
def analytics_top_searches(limit: int = Query(10, ge=1, le=100), days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT lower(trim(query)) AS q, count(*) AS searches
              FROM events
             WHERE event_type='search' AND query IS NOT NULL AND trim(query)<>''
               AND created_at >= now() - make_interval(days => %s)
             GROUP BY lower(trim(query)) ORDER BY searches DESC LIMIT %s
            """,
            (days, limit),
        )
        base = {
            r["q"]: {"query": r["q"], "searches": r["searches"], "clicks": 0, "orders": 0, "revenue": 0.0}
            for r in cursor.fetchall()
        }
        cursor.execute(
            """
            WITH ev AS (
              SELECT session_id, event_type, lower(trim(query)) AS q, value, created_at,
                     count(*) FILTER (WHERE event_type='search')
                       OVER (PARTITION BY session_id ORDER BY created_at ROWS UNBOUNDED PRECEDING) AS grp
                FROM events WHERE created_at >= now() - make_interval(days => %s)
            ),
            gq AS (SELECT session_id, grp, max(q) FILTER (WHERE event_type='search') AS q
                     FROM ev GROUP BY session_id, grp)
            SELECT gq.q AS q,
                   count(*) FILTER (WHERE ev.event_type='click')                     AS clicks,
                   count(DISTINCT ev.session_id) FILTER (WHERE ev.event_type='purchase') AS orders,
                   COALESCE(sum(ev.value) FILTER (WHERE ev.event_type='purchase'),0)  AS revenue
              FROM ev JOIN gq ON gq.session_id=ev.session_id AND gq.grp=ev.grp
             WHERE gq.q IS NOT NULL AND ev.grp >= 1
             GROUP BY gq.q
            """,
            (days,),
        )
        for r in cursor.fetchall():
            if r["q"] in base:
                base[r["q"]].update(clicks=r["clicks"] or 0, orders=r["orders"] or 0,
                                    revenue=round(float(r["revenue"] or 0), 2))
        data = []
        for v in sorted(base.values(), key=lambda x: -x["searches"]):
            s = v["searches"]
            data.append({
                "query": v["query"], "searches": s,
                "ctr_pct": round(v["clicks"] / s * 100) if s else 0,
                "conversion_pct": round(v["orders"] / s * 100, 1) if s else 0,
                "revenue": v["revenue"],
            })
        return {"success": True, "data": data}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/no-results  (Overview: "Searches with no results")
# ==========================
@app.get("/analytics/no-results")
def analytics_no_results(limit: int = Query(10, ge=1, le=100), days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT lower(trim(query)) AS query, count(*) AS count
              FROM events
             WHERE event_type IN ('search_no_result','search_no_results','no_results','no_result','zero_results')
               AND query IS NOT NULL AND trim(query)<>''
               AND created_at >= now() - make_interval(days => %s)
             GROUP BY lower(trim(query)) ORDER BY count DESC LIMIT %s
            """,
            (days, limit),
        )
        data = [{"query": r["query"], "count": r["count"]} for r in cursor.fetchall()]
        return {"success": True, "data": data}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/products  (Products catalog list, from OpenSearch)
# ==========================
@app.get("/analytics/products")
def analytics_products(limit: int = Query(200, ge=1, le=1000), search: str = ""):
    must = [{"term": {"is_visible": True}}]
    if search.strip():
        must.append({"multi_match": {"query": search.strip(), "fields": ["name^2", "brand", "category"]}})
    body = {
        "query": {"bool": {"must": must}},
        # unmapped_type keeps the sort (and the whole request) from 500-ing if the
        # index was created without a trending_score field on some docs.
        "sort": [{"trending_score": {"order": "desc", "unmapped_type": "float"}}],
        "size": limit,
        "_source": ["product_id", "name", "category", "price", "sale_price", "in_stock", "image_url"],
    }
    try:
        res = requests.post(
            f"{OPENSEARCH_URL}/{INDEX}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body),
            timeout=5,
        )
        hits = res.json().get("hits", {}).get("hits", []) if res.status_code == 200 else []
    except Exception as ex:
        print(f"[/analytics/products] OpenSearch error: {ex}")
        hits = []
    data = []
    for h in hits:
        s = h.get("_source", {})
        cat = s.get("category")
        if isinstance(cat, list):
            cat = cat[0] if cat else ""
        price = s.get("sale_price") or s.get("price") or 0
        data.append({
            "product_id": s.get("product_id", h.get("_id", "")),
            "name": s.get("name", ""),
            "category": cat or "",
            "price": float(price or 0),
            "in_stock": bool(s.get("in_stock", True)),
            "image_url": s.get("image_url", "") or "",
        })
    return {"success": True, "data": data}


# ==========================
# /analytics/top-products  (from product_metrics)
# ==========================
@app.get("/analytics/top-products")
def analytics_top_products(limit: int = Query(10, ge=1, le=100)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT product_id, views, clicks, add_to_cart, orders, wishlist, trending_score
              FROM product_metrics ORDER BY trending_score DESC LIMIT %s
            """,
            (limit,),
        )
        return {"success": True, "data": cursor.fetchall()}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/top-categories  (from OpenSearch; hardened so a mapping quirk can't 500)
# ==========================
@app.get("/analytics/top-categories")
def analytics_top_categories():
    query = {"size": 0, "aggs": {"categories": {"terms": {"field": "category.keyword", "size": 10}}}}
    try:
        res = requests.post(f"{OPENSEARCH_URL}/{INDEX}/_search", json=query, timeout=5)
        buckets = (
            res.json().get("aggregations", {}).get("categories", {}).get("buckets", [])
            if res.status_code == 200 else []
        )
    except Exception as ex:
        print(f"[/analytics/top-categories] OpenSearch error: {ex}")
        buckets = []
    return {"success": True, "data": buckets}


# ==========================
# /analytics/revenue
# ==========================
@app.get("/analytics/revenue")
def analytics_revenue(days: int = Query(30, ge=1, le=365)):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT COALESCE(sum(value) FILTER (WHERE event_type='purchase'),0)     AS revenue,
                   count(DISTINCT session_id) FILTER (WHERE event_type='purchase') AS orders
              FROM events WHERE created_at >= now() - make_interval(days => %s)
            """,
            (days,),
        )
        r = cursor.fetchone()
        return {"success": True, "data": {"revenue": round(float(r["revenue"] or 0), 2), "orders": r["orders"] or 0}}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/live-visitors
# ==========================
@app.get("/analytics/live-visitors")
def analytics_live_visitors():
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            "SELECT count(DISTINCT visitor_id) AS visitors FROM events WHERE created_at >= now() - interval '5 minutes'"
        )
        return {"success": True, "data": cursor.fetchone()}
    finally:
        cursor.close()
        db.close()


# ==========================
# /analytics/billing
# --------------------------
# Metered usage for the CURRENT CALENDAR MONTH, billed PER SEARCH (query),
# de-duplicated PER SESSION: the same query repeated within one session counts
# once (case-insensitive, trimmed). A genuine re-search in a different session
# counts again. Free allowance first, then a per-1,000 rate. All money math is
# server-side. (word_count column is left populated but unused for billing.)
# ==========================
@app.get("/analytics/billing")
def analytics_billing(
    free_allowance: int = Query(10000, ge=0),
    rate_per_1000: float = Query(0.40, ge=0),
):
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM (
                 SELECT DISTINCT session_id, lower(btrim(query)) AS q
                   FROM events
                  WHERE event_type IN ('search','search_no_result')
                    AND btrim(coalesce(query,'')) <> ''
                    AND created_at >= date_trunc('month', now())
                    AND created_at <  date_trunc('month', now()) + interval '1 month'
               ) d)                                                   AS searches_used,
              date_trunc('month', now())::date                        AS cycle_start,
              (date_trunc('month', now()) + interval '1 month' - interval '1 day')::date
                                                                      AS cycle_end,
              EXTRACT(DAY FROM (date_trunc('month', now()) + interval '1 month' - interval '1 day'))::int
                                                                      AS days_in_month,
              EXTRACT(DAY FROM now())::int                            AS day_of_month
            """
        )
        r = cursor.fetchone()
        searches_used     = int(r["searches_used"] or 0)
        billable_searches = max(0, searches_used - free_allowance)
        billable_units    = billable_searches / 1000.0
        estimated_total   = round(billable_units * rate_per_1000, 2)
        dim = int(r["days_in_month"] or 30)
        dom = int(r["day_of_month"] or 1)
        projected_searches = int(round(searches_used / dom * dim)) if dom > 0 else searches_used
        return {
            "success": True,
            "data": {
                "searches_used": searches_used,
                "free_allowance": free_allowance,
                "billable_searches": billable_searches,
                "rate_per_1000": rate_per_1000,
                "billable_units": round(billable_units, 2),
                "estimated_total": estimated_total,
                "projected_searches": projected_searches,
                "cycle_start": r["cycle_start"].isoformat() if r["cycle_start"] else None,
                "cycle_end": r["cycle_end"].isoformat() if r["cycle_end"] else None,
                "dedupe": "per_session",
            },
        }
    finally:
        cursor.close()
        db.close()
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from typing import Any
import hashlib
from time import perf_counter
import logging

from dotenv import load_dotenv
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
# from pydantic.v1.networks import host_regex
from redis import Redis
from redis.exceptions import RedisError
# from unicodedata import normalize

from app.database import (
    close_db_pool,
    fetch_last_logs,
    init_db_pool,
    # write_logs,
    fetch_categories,
    search_products,
    write_request_log)



load_dotenv()

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db_pool()
    init_redis_cache()
    try:
        yield
    finally:
        close_redis_cache()
        close_db_pool()


app = FastAPI(title="Simple Round Site", version="0.0.1", lifespan=lifespan)
WORKER_NAME = os.getenv("WORKER_NAME")
WORKER_CACHE_TTL = int(os.getenv("WORKER_CACHE_TTL"))
WORKER_CACHE_KEY = "workers:api:worker"
SEARCH_CACHE_PREFIX = "search:products:"
SEARCH_CACHE_TTL_SECONDS = max(int(os.getenv("SEARCH_CACHE_TTL", "30")), 1)
_redis_client: Redis | None = None

#Redis
def init_redis_cache() -> None:
    global _redis_client
    host = os.getenv("REDIS_HOST", "redis")
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")

    client = Redis(host=host, port=int(port), db=int(db), decode_responses=True)
    try:
        client.ping()
    except RedisError:
        _redis_client = None
        return
    _redis_client = client

def close_redis_cache() -> None:
    global _redis_client
    if _redis_client:
        _redis_client.close()
        _redis_client = None

def _read_worker_cache() -> dict[str, Any] | None:
    if _redis_client is None:
        return None

    try:
        value = _redis_client.get(WORKER_CACHE_KEY)
    except RedisError:
        return None

    if value is None:
        return None

    try:
        payload = json.loads(value)
    except json.decoder.JSONDecodeError:
        return None

    if not isinstance(payload, dict): #if isinstance(payload, dict):
        return None

    worker = payload.get("worker")
    request_time = payload.get("time")

    if not isinstance(worker, str) or not isinstance(request_time, str):
        return None

    return {"worker": worker, "time": request_time}

def _write_worker_cache(worker: str, request_time: str) -> None:
    if _redis_client is None:
        return

    payload = {"worker": worker, "time": request_time}

    try:
        _redis_client.setex(WORKER_CACHE_KEY, WORKER_CACHE_TTL, json.dumps(payload))
    except RedisError:
        return

def _search_cache_key(query: str, category: str | None, limit: int) -> str:
    normalized_query = query.strip().lower() if query else ""
    normalized_category = category.strip().lower() if category else ""
    digest = hashlib.sha256(f"{normalized_query}|{normalized_category}|{limit}".encode("utf-8")).hexdigest()
    return f"{SEARCH_CACHE_PREFIX}{digest}"

def _read_search_cache(query: str, category: str | None, limit: int) -> dict[str, object] | None:
    if _redis_client is None:
        return None

    key = _search_cache_key(query=query, category=category, limit=limit)
    try:
        raw_value = _redis_client.get(key)
    except RedisError:
        return None

    if raw_value is None:
        return None

    try:
        payload = json.loads(raw_value)
    except json.decoder.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    items = payload.get("items")
    count = payload.get("count")
    if not isinstance(items, list) or not isinstance(count, int):
        return None
    return {"items": items, "count": count}

def _write_search_cache(query: str, category: str | None, limit: int, items: list[dict[str,Any]]) -> None:
    if _redis_client is None:
        return

    key = _search_cache_key(query=query, category=category, limit=limit)
    payload = {"items": items, "count": len(items)}
    try:
        _redis_client.setex(key, SEARCH_CACHE_TTL_SECONDS, json.dumps(payload))
    except RedisError:
        return


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Marketplace Demo</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f4f6f8; }
            .layout { display: grid; gap: 20px; max-width: 1000px; margin: 0 auto; }
            .card { background: white; padding: 24px; border-radius: 12px; box-shadow: 0 4px 18px rgba(0,0,0,.08); }
            button { padding: 10px 16px; border: 0; border-radius: 8px; cursor: pointer; background: #0f62fe; color: #fff; }
            button:hover { background: #1d4ed8; }
            input, select { padding: 10px; border-radius: 8px; border: 1px solid #c9d2de; }
            code { background: #eef2ff; padding: 2px 6px; border-radius: 6px; }
            #history { margin-top: 16px; }
            .search-row { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
            #searchMeta { color: #4e5d6c; margin: 8px 0 12px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { border-bottom: 1px solid #e6ebf1; text-align: left; padding: 8px; font-size: 14px; }
            th { background: #f8fafc; }
            @media (max-width: 700px) {
                body { margin: 16px; }
                th, td { font-size: 13px; }
            }
        </style>
    </head>
    <body>
        <div class="layout">
            <div class="card">
                <h1>Round Robin Between 2 Workers</h1>
                <p>Request goes through Nginx to workers <code>worker-1</code> and <code>worker-2</code>.</p>
                <p>Responses from <code>/api/worker</code> are cached in Redis for a short TTL.</p>
                <p>Each worker writes a log row to PostgreSQL table <code>request_logs</code>.</p>
                <button onclick="hitWorker()">Send request</button>
                <p id="current">No requests yet.</p>
                <div id="history"></div>
            </div>

            <div class="card">
                <h2>Marketplace Search</h2>
                <p>Database is prefilled with 1000 random products in 10 categories.</p>
                <div class="search-row">
                    <input id="queryInput" type="text" placeholder="Search products" />
                    <select id="categorySelect">
                        <option value="">All categories</option>
                    </select>
                    <button onclick="searchProducts()">Search</button>
                </div>
                <div id="searchMeta"></div>
                <div id="productsTable"></div>
            </div>
        </div>

        <script>
            async function hitWorker() {
                const res = await fetch('/api/worker', { cache: 'no-store' });
                const data = await res.json();
                const cachedSuffix = data.cached ? " (cached)" : "";
                document.getElementById('current').textContent = `Handled by: ${data.worker} at ${data.time}${cachedSuffix}`;
                const row = document.createElement('div');
                row.textContent = `${new Date().toLocaleTimeString()} -> ${data.worker}${cachedSuffix}`;
                document.getElementById('history').prepend(row);
            }

            function renderProducts(items) {
                const container = document.getElementById('productsTable');
                container.innerHTML = '';

                if (!items.length) {
                    container.textContent = 'No products found.';
                    return;
                }

                const table = document.createElement('table');

                const thead = document.createElement('thead');
                thead.innerHTML = `
                    <tr>
                        <th>Name</th>
                        <th>Category</th>
                        <th>Price</th>
                        <th>Stock</th>
                    </tr>
                `;
                table.appendChild(thead);

                const tbody = document.createElement('tbody');

                items.forEach((item) => {
                    const tr = document.createElement('tr');

                    const name = document.createElement('td');
                    name.textContent = item.name;
                    tr.appendChild(name);

                    const category = document.createElement('td');
                    category.textContent = item.category;
                    tr.appendChild(category);

                    const price = document.createElement('td');
                    price.textContent = `$${item.price}`;
                    tr.appendChild(price);

                    const stock = document.createElement('td');
                    stock.textContent = String(item.stock);
                    tr.appendChild(stock);

                    tbody.appendChild(tr);
                });

                table.appendChild(tbody);
                container.appendChild(table);
            }

            async function loadCategories() {
                const res = await fetch('/api/categories', { cache: 'no-store' });
                const data = await res.json();
                const select = document.getElementById('categorySelect');

                data.items.forEach((category) => {
                    const option = document.createElement('option');
                    option.value = category;
                    option.textContent = category;
                    select.appendChild(option);
                });
            }

            async function searchProducts() {
                const query = document.getElementById('queryInput').value.trim();
                const category = document.getElementById('categorySelect').value;
                const params = new URLSearchParams({ limit: '30' });
                if (query) params.set('q', query);
                if (category) params.set('category', category);

                const res = await fetch(`/api/products/search?${params.toString()}`, { cache: 'no-store' });
                const data = await res.json();

                document.getElementById('searchMeta').textContent = `Found: ${data.count} in ${data.search_ms} ms`;
                renderProducts(data.items);
            }

            document.addEventListener('DOMContentLoaded', async () => {
                await loadCategories();
                await searchProducts();
            });
        </script>
    </body>
    </html>
    """

@app.get("/api/worker")
def who_handler_request(request: Request) -> dict[str, object]: #в скобках после запятой было "str, Any"
    cached_response = _read_worker_cache()
    if cached_response is not None:
        return cached_response

    client_ip = request.client.host if request.client else None
    write_request_log(worker_name=WORKER_NAME, path=request.url.path, client_ip=client_ip)
    request_time = datetime.now(timezone.utc).isoformat()
    _write_worker_cache(worker=WORKER_NAME, request_time=request_time)
    return {
        "worker": WORKER_NAME,
        "time": request_time,
        "cached": False,
    }
    # write_logs(worker_name=WORKER_NAME, client_ip=client_ip, path=request.url.path)
    # return {"worker": WORKER_NAME,
    #         "time": datetime.now(timezone.utc).isoformat(),
    #         }

@app.post("/api/logs")
def get_logs(limit: int=20) -> dict[str, object]:
    safe_limit = min(max(1, limit), 100)
    return {"items": fetch_last_logs(limit=safe_limit)}

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "worker": WORKER_NAME}

@app.get("/api/categories")
def get_marketplace_categories() -> dict[str, object]:
    return {"items":fetch_categories()}

@app.get("/api/products/search")
def search_marketplace_products(
    q: str | None = None,
    category: str | None = None,
    limit: int = 20
) -> dict[str, object]:
    safe_limit = min(max(1, limit), 100)
    started_at = perf_counter()
    cached_payload = _read_search_cache(query=q, category=category, limit=safe_limit)
    if cached_payload is not None:
        elapsed_time = round((perf_counter() - started_at) * 1000, 2)
        logger.info(f"source: redis; time: {elapsed_time}")
        return {
            "items": cached_payload["items"],
            "count": cached_payload["count"],
            "search_ms": elapsed_time,
        }

    items = search_products(q, category, safe_limit)
    _write_search_cache(query=q, category=category, limit=safe_limit, items=items)
    elapsed_time = round((perf_counter() - started_at) * 1000, 2)
    logger.info(f"source: db; time: {elapsed_time}")
    return {
        "items": items,
        "count": len (items),
        "search_ms": elapsed_time
    }




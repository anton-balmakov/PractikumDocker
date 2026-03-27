from dotenv import load_dotenv
import os
from typing import Any

from psycopg2 import pool

_db_pool: pool.SimpleConnectionPool | None = None
_MIGRATION_LOCK_KEY = 42424201
_MARKETPLACE_CATEGORIES = (
    "Electronics",
    "Home",
    "Fashion",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
    "Automotive",
    "Food",
    "Pets",
)

def _dsn() -> str:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    return f"host={host} port={port} user={user} password={password} dbname={db_name}"

def init_db_pool() -> None: #не вызывается _ensure_tables() Значит таблицы request_logs, categories, products при новом чистом запуске не создадутся автоматически
    global _db_pool
    if _db_pool is not None:
        return
    _db_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, dsn=_dsn())
    _ensure_tables()


def close_db_pool() -> None:
    global _db_pool
    if _db_pool is not None:
        _db_pool.closeall()
        _db_pool = None

def _ensure_tables() -> None:
    if _db_pool is None:
        return

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK_KEY,))
            cursor.execute( #в таблице отсутсвует id но в функции fetch_last_logs идет выбор id (Либо добавлять id BIGSERIAL PRIMARY KEY в таблицу, либо убирать id из select.)
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                id BIGSERIAL PRIMARY KEY, 
                worker_name VARCHAR(100) NOT NULL,
                path VARCHAR(255) NOT NULL,
                client_ip VARCHAR(64),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE
                );
                """
           )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                id BIGSERIAL PRIMARY KEY,
                category_id INT NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
                name VARCHAR(255) NOT NULL UNIQUE,
                description TEXT NOT NULL,
                price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
                stock INT NOT NULL CHECK (stock >= 0),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_category_id ON products(category_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_name_lower ON products(LOWER(name));")

            cursor.executemany(
                """
                INSERT INTO categories (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING               
                """,
                [(category,) for category in _MARKETPLACE_CATEGORIES],
            )

            cursor.execute(
                """
                WITH needed_rows AS (
                    SELECT GREATEST(0, 1000 - COUNT(*) ) AS cnt
                    FROM products
                ),
                series AS (
                    SELECT generate_series(1, (SELECT cnt FROM needed_rows)) AS n
                ),
                generated AS (
                    SELECT
                        n,
                        (ARRAY[
                            'Smart', 'Ultra', 'Eco', 'Classic', 'Pro', 'Compact', 'Premium', 'Daily', 'Urban', 'Flex'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS adjective,
                        (ARRAY[
                            'Portable', 'Modern', 'Advanced', 'Comfort', 'Essential', 'Travel', 'Performance', 'Studio', 'Family', 'Lite'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS style,
                        (ARRAY[
                          'Bundle', 'Set', 'Kit', 'Pack', 'Edition', 'Model', 'Series', 'Selection', 'Version', 'Collection'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS noun,
                        (ARRAY[
                            'Electronics', 'Home', 'Fashion', 'Beauty', 'Sports', 'Books', 'Toys', 'Automotive', 'Food', 'Pets'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS category_name
                    FROM series
                )
                INSERT INTO products (category_id, name, description, price, stock)
                SELECT
                    c.id,
                    CONCAT(g.adjective, ' ', g.style, ' ', g.noun, ' #', g.n),
                    CONCAT('Marketplace product #', g.n, ' in category ', g.category_name, '.'),
                    ROUND((5 + RANDOM() * 1995)::NUMERIC, 2),
                    (10 + FLOOR(RANDOM() * 190))::INT
                FROM generated g
                JOIN categories c ON c.name = g.category_name;
                """
            )

            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

    finally:
        _db_pool.putconn(conn)


def write_request_log(worker_name: str, path: str, client_ip: str | None) -> None:
    if _db_pool is None:
        return

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO request_logs (worker_name, path, client_ip)
                VALUES (%s, %s, %s)
                """,
                (worker_name, path, client_ip),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _db_pool.putconn(conn)

def fetch_last_logs(limit: int = 20) -> list[dict[str, Any]]:
    if _db_pool is None:
        return []

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, worker_name, path, client_ip, created_at
                FROM request_logs
                ORDER BY id DESC 
                LIMIT %s                             
                """,
                (limit,),
            )
        rows = cursor.fetchall()
        return [
            {"id": row[0],
             "worker_name": row[1],
             "path": row[2],
             "client_ip": row[3],
             "created_at": row[4].isoformat(),
             }
            for row in rows
        ]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _db_pool.putconn(conn)


def fetch_categories() -> list[str]:
    if _db_pool is None:
        return []
    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT name FROM categories ORDER BY name;")
            rows= cursor.fetchall()
        return [row[0] for row in rows]
    finally:
        _db_pool.putconn(conn)


def search_products(query: str, category: str | None=None, limit: int = 20) -> list[dict[str, Any]]:
    if _db_pool is None:
        return []

    normalized_query = query.strip().lower() if query else ""
    query_pattern = f"%{normalized_query}%"
    normalize_category = category.strip() if category else None
    if normalize_category == "":
        normalize_category = None

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT p.id, p.name, p.description, p.price, p.stock, c.name
                FROM products p
                JOIN categories c ON c.id = p.category_id
                WHERE
                    (%s= '' OR p.name ILIKE %s OR p.description ILIKE %s)
                    AND (%s is NULL OR c.name = %s)
                ORDER BY p.id DESC
                LIMIT %s
                """,
                (normalized_query, query_pattern, query_pattern, normalize_category, normalize_category, limit),
            )
            rows = cursor.fetchall()
        return [
            {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "price": float(row[3]),
                "stock": row[4],
                "category": row[5],
            }
            for row in rows
        ]
    finally:
        _db_pool.putconn(conn)






from dotenv import load_dotenv
import os
from typing import Any

from psycopg2 import pool

_db_pool: pool.SimpleConnectionPool | None = None

def _dsn() -> str:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    return f"host={host} port={port} user={user} password={password} dbname={db_name}"

def init_db_pool() -> None:
    global _db_pool
    if _db_pool is not None:
        return
    _db_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, dsn=_dsn())


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
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                worker_name VARCHAR(100) NOT NULL,
                path VARCHAR(255) NOT NULL,
                client_ip VARCHAR(64),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            conn.commit()
    finally:
        _db_pool.putconn(conn)

def write_logs(worker_name: str, path: str, client_ip: str | None) -> None:
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
             "work_name": row[1],
             "path": row[2],
             "client_ip": row[3],
             "create_at": row[4].isoformat(),
             }
            for row in rows
        ]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _db_pool.putconn(conn)








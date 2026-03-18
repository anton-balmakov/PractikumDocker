from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from app.database import close_db_pool, fetch_last_logs, init_db_pool, write_logs

load_dotenv()

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db_pool()
    try:
        yield
    finally:
        close_db_pool()

app = FastAPI(title="Simple Round Site", version="0.0.1", lifespan=lifespan)
WORKER_NAME = os.getenv("WORKER_NAME")

@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
    <!doctype html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\" />
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
      <title>Simple Round Demo</title>
      <style>
        body {
            font-family: Arial, sans-serif;
            margin: 40px;
            background: #f4f6f8;
          }
        
          .card {
            max-width: 700px;
            margin: 0 auto;
            background: white;
            padding: 24px;
            border-radius: 12px;
            box-shadow: 0 4px 18px rgba(0,0,0,0.08);
          }
        
          button {
            padding: 10px 16px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            background: #2563eb;
            color: white;
            font-size: 14px;
          }
        
          button:hover {
            background: #1d4ed8;
          }
        
          code {
            background: #eef2ff;
            padding: 2px 6px;
            border-radius: 6px;
          }
        
          #history {
            margin-top: 16px;
          }
      </style>
    </head>
    <body>
      <div class=\"card\">
        <h1>Round Robin Between 2 Workers</h1>
        <p>Request goes through Nginx to workers <code>worker-1</code> and <code>worker-2</code>.</p>
        <p>Each worker writes s log row to PostgreSQL table <code>request_logs</code>.</p>
        <button onclick=\"hitWorker()\">Send request</button>
        <p id=\"current\">No request yet.</p>
        <div id=\"history\"></div>
      <div>

    <script>
      async function hitWorker() {
        const res = await fetch('/api/worker', { cache: 'no-store' });
        const data = await res.json();
        document.getElementById('current').textContent = `Handler by: ${data.worker} at ${data.time}`;
        const row = document.createElement('div');
        row.textContent = `${new Date().toLocaleTimeString()} -> ${data.worker}`;
        document.getElementById('history').prepend(row);
        }
    </script>
    </body>
    </html>
    """

@app.get("/api/worker")
def who_handler_request(request: Request) -> dict[str, str]:
    client_ip = request.client.host if request.client else None
    write_logs(worker_name=WORKER_NAME, client_ip=client_ip, path=request.url.path)
    return {"worker": WORKER_NAME,
            "time": datetime.now(timezone.utc).isoformat(),
            }

@app.post("/api/logs")
def get_logs(limit: int=20) -> dict[str, object]:
    safe_limit = min(max(1, limit), 100)
    return {"items": fetch_last_logs(limit=safe_limit)}

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "worker": WORKER_NAME}





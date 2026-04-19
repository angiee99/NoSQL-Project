#!/bin/bash
set -euo pipefail

echo "[data-import] Installing Python dependencies..."
pip install --no-cache-dir -r /app/import/requirements.txt

echo "[data-import] Waiting for projectdb schema and auth..."

until python - <<'PY'
import os
from pymongo import MongoClient

uri = os.environ["MONGO_URI"]
client = MongoClient(uri, serverSelectionTimeoutMS=3000)

try:
    client.admin.command("ping")
    db = client["projectdb"]
    names = db.list_collection_names()
    ok = all(name in names for name in ["patients", "encounters", "claims"])
    print(f"collections={names}")
    raise SystemExit(0 if ok else 1)
except Exception as e:
    print(e)
    raise SystemExit(1)
PY
do
  sleep 3
done

echo "[data-import] Starting CSV import..."
python /app/import/import_data.py

echo "[data-import] Done."
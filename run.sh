#!/usr/bin/env bash

set -euo pipefail

# Ensure the project root is the current working directory.
cd "$(dirname "$0")"

# Create and activate the local virtual environment.
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source ".venv/bin/activate"

# Install dependencies if Uvicorn is not available in the venv.
if ! command -v uvicorn >/dev/null 2>&1; then
  pip install -r requirements.txt
fi

# Find the first available port starting at 8000.
find_free_port(){
  python3 - <<'PY'
import socket
import sys
for port in range(8000, 8100):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(('127.0.0.1', port))
        except OSError:
            continue
    print(port)
    sys.exit(0)
print('')
sys.exit(1)
PY
}

port=$(find_free_port)
if [ -z "$port" ]; then
  echo "No available port found between 8000 and 8099." >&2
  exit 1
fi

echo "Starting on http://127.0.0.1:$port"
PYTHONPATH="src" exec uvicorn main:app --reload --host 127.0.0.1 --port "$port"

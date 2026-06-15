# flight_route_tracker

A FastAPI + Uvicorn flight route tracker proxy for ADS-B data providers.

## Setup

1. Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

Start the app with the bundled script:

```bash
./run.sh
```

The script will automatically create and activate `./.venv` if needed, and install dependencies from `requirements.txt` when Uvicorn is not already available.

Then open `http://127.0.0.1:8000` in your browser.

## Project structure

- `src/main.py` - FastAPI application and ADS-B proxy logic.
- `requirements.txt` - Python dependencies.
- `run.sh` - Launches Uvicorn for development.

## Notes

- The app proxies ADS-B requests from `adsb.fi` and `adsb.lol`.
- It uses server-side caching and request throttling to stay within upstream limits.
- FastAPI serves a simple Leaflet-based frontend at `/`.

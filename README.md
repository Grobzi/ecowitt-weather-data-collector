# Ecowitt weatherstation FTPS uploader

This repository contains a small Python service that receives Ecowitt gateway POST updates, aggregates weather values in 15-minute windows, and uploads a compact JSON history to FTPS (explicit TLS).

Quick start
1. Create a Python virtualenv and install requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

2. Copy `config/settings.json.example` to `config/settings.json` and adjust the local service settings.
3. Copy `config/.env.example` to `config/.env` and fill in `FTPS_*` values.
4. Run:

```bash
python main.py --settings config/settings.json --env config/.env
```

5. Install as a systemd service on Linux:

```bash
chmod +x install-systemd-service.sh
./install-systemd-service.sh
```

Settings (`config/settings.json`)

| Field | Default | Description |
|---|---|---|
| `port` | `8000` | Local HTTP port for incoming Ecowitt POST payloads |
| `interval_min` | `15` | Aggregation interval in minutes |
| `remote_file` | `weather_compact.json` | Remote FTPS target filename |

Environment variables (`.env`)

| Variable | Default | Description |
|---|---|---|
| `FTPS_HOST` | *(required)* | FTPS server hostname |
| `FTPS_PORT` | `21` | FTPS server port |
| `FTPS_USER` | *(required)* | FTPS username |
| `FTPS_PASSWORD` | | FTPS password |
| `FTPS_CAFILE` | | Optional custom CA certificate path |
| `FTPS_TLS_HOSTNAME` | | Optional SNI/verification hostname override |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

Security
- Do not commit `config/.env`.
- Use FTPS credentials with least privilege.

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="weatherstation.service"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
SETTINGS_PATH="${SCRIPT_DIR}/config/settings.json"
ENV_PATH="${SCRIPT_DIR}/config/.env"
SERVICE_USER="${SUDO_USER:-$(id -un)}"
SERVICE_GROUP="$(id -gn "${SERVICE_USER}")"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi

if [[ -z "${PYTHON_BIN}" ]]; then
    echo "python3 not found" >&2
    exit 1
fi

if [[ ! -f "${SETTINGS_PATH}" ]]; then
    echo "Missing settings file: ${SETTINGS_PATH}" >&2
    echo "Copy config/settings.json.example to config/settings.json first." >&2
    exit 1
fi

if [[ ! -f "${ENV_PATH}" ]]; then
    echo "Missing env file: ${ENV_PATH}" >&2
    echo "Copy config/.env.example to config/.env first." >&2
    exit 1
fi

cat <<EOF | sudo tee "${UNIT_PATH}" >/dev/null
[Unit]
Description=Weather Station
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${SCRIPT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${PYTHON_BIN} ${SCRIPT_DIR}/main.py --settings ${SETTINGS_PATH} --env ${ENV_PATH}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}"

echo "Installed and started ${SERVICE_NAME}"
echo "Logs: journalctl -u ${SERVICE_NAME} -f"

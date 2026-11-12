#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/mnt/Project/Fund-ANA"
SERVICE_NAME="fund-ana-scheduler.service"
SERVICE_SRC="$PROJECT_ROOT/deploy/systemd/$SERVICE_NAME"
SERVICE_DST="/etc/systemd/system/$SERVICE_NAME"

if [[ ! -f "$SERVICE_SRC" ]]; then
  echo "service template not found: $SERVICE_SRC" >&2
  exit 1
fi

if [[ ! -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  echo "python not found in $PROJECT_ROOT/.venv/bin/python" >&2
  exit 1
fi

cp "$SERVICE_SRC" "$SERVICE_DST"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
systemctl --no-pager --lines=20 status "$SERVICE_NAME"

main_pid="$(systemctl show -p MainPID --value "$SERVICE_NAME" | tr -d '[:space:]')"
if [[ -z "$main_pid" || "$main_pid" == "0" ]]; then
  echo "service main pid not ready: $SERVICE_NAME" >&2
  exit 1
fi

sleep 2

if ! ps -p "$main_pid" -o cmd= | grep -F "scripts/run_scheduler.py" >/dev/null; then
  echo "scheduler service did not start the expected command" >&2
  exit 1
fi

echo "fund-ana-scheduler.service installed and healthy"

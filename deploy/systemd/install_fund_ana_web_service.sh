#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/mnt/Project/Fund-ANA"
SERVICE_NAME="fund-ana-web.service"
SERVICE_SRC="$PROJECT_ROOT/deploy/systemd/$SERVICE_NAME"
SERVICE_DST="/etc/systemd/system/$SERVICE_NAME"

if [[ ! -f "$SERVICE_SRC" ]]; then
  echo "service template not found: $SERVICE_SRC" >&2
  exit 1
fi

if [[ ! -x "$PROJECT_ROOT/.venv/bin/gunicorn" ]]; then
  echo "gunicorn not found in $PROJECT_ROOT/.venv/bin/gunicorn" >&2
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

for _ in $(seq 1 15); do
  if ss -ltnp 2>/dev/null | grep -F ":8090" | grep -F "pid=$main_pid" >/dev/null; then
    break
  fi
  sleep 1
done

if ! ss -ltnp 2>/dev/null | grep -F ":8090" | grep -F "pid=$main_pid" >/dev/null; then
  echo "service did not take ownership of port 8090; stop conflicting process first" >&2
  exit 1
fi

curl -fsS "http://127.0.0.1:8090/api/health" >/dev/null
echo "fund-ana-web.service installed and healthy"

#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p /app/backend/data /app/reports /app/logs /app/core/logs

exec gunicorn \
  --workers "${FUND_ANA_GUNICORN_WORKERS:-1}" \
  --threads "${FUND_ANA_GUNICORN_THREADS:-8}" \
  --bind "0.0.0.0:${FUND_ANA_WEB_PORT:-8090}" \
  --access-logfile - \
  --error-logfile - \
  backend.app:app

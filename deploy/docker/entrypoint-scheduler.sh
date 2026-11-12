#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p /app/backend/data /app/reports /app/logs /app/core/logs

exec python /app/scripts/run_scheduler.py --force

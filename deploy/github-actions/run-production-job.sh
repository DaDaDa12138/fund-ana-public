#!/bin/sh
# Install root-owned in /usr/local/libexec/fund-ana-github-job; no checkout code runs as root.
set -eu
[ "$#" -eq 3 ] || exit 64
case "$1" in ''|*[!0-9]*) exit 64;; esac
case "$2" in ''|*[!0-9]*) exit 64;; esac
[ "${#1}" -le 20 ] && [ "${#2}" -le 5 ] || exit 64
case "$3" in daily_refresh|intraday_refresh|publish_only) ;; *) exit 64;; esac
cd /mnt/Project/Fund-ANA
exec /mnt/Project/Fund-ANA/.venv/bin/python \
  /mnt/Project/Fund-ANA/scripts/run_github_daily_job.py \
  --github-run-id "$1" --attempt "$2" --job "$3"

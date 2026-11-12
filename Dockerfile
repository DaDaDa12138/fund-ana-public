FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata ca-certificates zstd libseccomp2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-runtime.txt /app/requirements-runtime.txt
COPY requirements-analytics.txt /app/requirements-analytics.txt

RUN pip install --upgrade pip \
    && pip install -r /app/requirements-runtime.txt

COPY backend /app/backend
COPY config /app/config
COPY core /app/core
COPY deploy /app/deploy
COPY frontend /app/frontend
COPY scripts /app/scripts
COPY tests /app/tests

RUN python -c "import json, sys; from datetime import datetime; from zoneinfo import ZoneInfo; from pathlib import Path; sys.path.insert(0, '/app'); from core.version import VERSION_INFO, __version__; Path('/app/build_info.json').write_text(json.dumps({'build_time': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='seconds'), 'code_version': __version__, 'title': VERSION_INFO.get('title'), 'image_tag': f'fund-ana:{__version__}'}, ensure_ascii=False, indent=2), encoding='utf-8')"

RUN mkdir -p /app/backend/data /app/reports /app/logs /app/core/logs \
    && chmod +x /app/deploy/docker/entrypoint-web.sh /app/deploy/docker/entrypoint-scheduler.sh

EXPOSE 8090

CMD ["/app/deploy/docker/entrypoint-web.sh"]

FROM python:3.10-alpine

LABEL maintainer="Horst Gutmann <horst@zerokspot.com>"

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements/ /app/requirements/
RUN python -m pip install --no-cache-dir \
    -r /app/requirements/promclient050.txt \
    -r /app/requirements/celery6.txt

COPY celery_prometheus_exporter.py docker-entrypoint.sh /app/

ENTRYPOINT ["/bin/sh", "/app/docker-entrypoint.sh"]
CMD []

EXPOSE 8888

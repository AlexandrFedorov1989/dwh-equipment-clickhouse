#!/usr/bin/env bash
set -euo pipefail

# Создаёт бакет ch-cold в MinIO (идемпотентно) — cold-tier для TTL ClickHouse.
# MinIO должен быть запущен: docker compose up -d / start
# Учётки — из .env (без дефолтов-секретов в скрипте).

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

: "${COMPOSE_PROJECT_NAME:?Задай COMPOSE_PROJECT_NAME в .env}"
: "${MINIO_ROOT_USER:?Задай MINIO_ROOT_USER в .env}"
: "${MINIO_ROOT_PASSWORD:?Задай MINIO_ROOT_PASSWORD в .env}"

NETWORK="${COMPOSE_PROJECT_NAME}_default"

docker run --rm --network "$NETWORK" \
  --entrypoint /bin/sh minio/mc:RELEASE.2024-08-17T11-33-50Z -c "
    mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} &&
    mc mb --ignore-existing local/ch-cold &&
    mc ls local
  "

echo "Бакеты готовы: ch-cold"

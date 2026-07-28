#!/usr/bin/env bash

set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Environment file not found: $ENV_FILE" >&2
  echo "Create it with: cp .env.example .env" >&2
  exit 1
fi

docker compose \
  --env-file "$ENV_FILE" \
  -f docker/compose.yml \
  -f docker/compose.postgres.yml \
  -f docker/compose.kafka-ui.yml \
  "$@"
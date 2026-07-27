#!/usr/bin/env bash

set -euo pipefail

docker compose \
  -f docker/compose.yml \
  -f docker/compose.postgres.yml \
  -f docker/compose.kafka-ui.yml \
  "$@"

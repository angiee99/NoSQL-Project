#!/bin/bash
set -euo pipefail

PORT="${MONGO_PORT:-27017}"
PASSWORD="${MONGO_BOOTSTRAP_PASSWORD:-}"

# First boot phase: localhost exception may still allow unauthenticated access.
if mongosh --host localhost --port "${PORT}" --quiet \
  --eval 'quit(db.adminCommand({ ping: 1 }).ok ? 0 : 1)' >/dev/null 2>&1; then
  exit 0
fi

# After bootstrap user exists, fall back to authenticated ping.
if [ -n "${PASSWORD}" ]; then
  if mongosh --host localhost --port "${PORT}" \
    -u "bootstrapAdmin" \
    -p "${PASSWORD}" \
    --authenticationDatabase admin \
    --quiet \
    --eval 'quit(db.adminCommand({ ping: 1 }).ok ? 0 : 1)' >/dev/null 2>&1; then
    exit 0
  fi
fi

exit 1
#!/bin/bash
set -e

wait_for_auth() {
  local host="$1"
  local port="$2"

  until mongosh --host "$host" --port "$port" \
    -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
    --eval "db.adminCommand({ ping: 1 })" --quiet >/dev/null 2>&1; do
    sleep 2
  done
}

wait_for_auth cfg1 27019
wait_for_auth mongo1 27017
wait_for_auth mongo4 27017
wait_for_auth mongo7 27017

until mongosh --host mongos --port 27017 \
  -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
  --eval "db.adminCommand({ ping: 1 })" --quiet >/dev/null 2>&1; do
  sleep 2
done

MONGO_CLUSTER_ADMIN_PASSWORD="$MONGO_CLUSTER_ADMIN_PASSWORD" \
MONGO_APP_PASSWORD="$MONGO_APP_PASSWORD" \
mongosh --host mongos --port 27017 \
  -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
  /scripts/init-cluster.js
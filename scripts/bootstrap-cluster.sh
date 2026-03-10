#!/bin/bash
set -e

wait_for_auth() {
  local host="$1"
  local port="$2"

  until mongosh --host "$host" --port "$port" \
    -u admin -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
    --eval "db.adminCommand({ ping: 1 })" --quiet >/dev/null 2>&1; do
    sleep 2
  done
}

wait_for_auth cfg1 27019
wait_for_auth mongo1 27017

until mongosh --host mongos --port 27017 \
  -u admin -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
  --eval "db.adminCommand({ ping: 1 })" --quiet >/dev/null 2>&1; do
  sleep 2
done

mongosh --host mongos --port 27017 \
  -u admin -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin \
  /scripts/init-cluster.js

echo "Cluster bootstrap finished."
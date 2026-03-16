#!/bin/bash
set -e

wait_for_auth() {
  local host="$1"
  local port="$2"

  echo "Waiting for authenticated ping on $host:$port ..."
  until mongosh --host "$host" --port "$port" \
    -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
    --quiet --eval "db.adminCommand({ ping: 1 }).ok" | grep -q 1; do
    sleep 2
  done
  echo "$host:$port accepts authenticated connections"
}

wait_for_rs_primary() {
  local host="$1"
  local port="$2"

  echo "Waiting for replica set PRIMARY visible from $host:$port ..."
  until mongosh --host "$host" --port "$port" \
    -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
    --quiet --eval '
      try {
        const s = rs.status();
        s.members.some(m => m.stateStr === "PRIMARY") ? print(1) : print(0);
      } catch (e) {
        print(0);
      }
    ' | grep -q 1; do
    sleep 3
  done
  echo "Replica set at $host:$port has a PRIMARY"
}

wait_for_auth cfg1 27019
wait_for_auth mongo1 27017
wait_for_auth mongo4 27017
wait_for_auth mongo7 27017

wait_for_rs_primary cfg1 27019
wait_for_rs_primary mongo1 27017
wait_for_rs_primary mongo4 27017
wait_for_rs_primary mongo7 27017

echo "Waiting for authenticated mongos ..."
until mongosh --host mongos --port 27017 \
  -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
  --quiet --eval "db.adminCommand({ ping: 1 }).ok" | grep -q 1; do
  sleep 2
done
echo "mongos is reachable"

MONGO_CLUSTER_ADMIN_PASSWORD="$MONGO_CLUSTER_ADMIN_PASSWORD" \
MONGO_APP_PASSWORD="$MONGO_APP_PASSWORD" \
mongosh --host mongos --port 27017 \
  -u bootstrapAdmin -p "$MONGO_BOOTSTRAP_PASSWORD" --authenticationDatabase admin \
  /scripts/init-cluster.js
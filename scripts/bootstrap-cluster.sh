#!/bin/bash
set -euo pipefail

BOOTSTRAP_USER="bootstrapAdmin"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-240}"

mongo_auth_eval() {
  local uri="$1"
  local eval_script="$2"

  mongosh "$uri" \
    -u "$BOOTSTRAP_USER" \
    -p "$MONGO_BOOTSTRAP_PASSWORD" \
    --authenticationDatabase admin \
    --quiet \
    --eval "$eval_script"
}

wait_for_rs_auth() {
  local label="$1"
  local uri="$2"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  echo "Waiting for authenticated access to ${label}..."

  until mongo_auth_eval "$uri" '
    try {
      const hello = db.adminCommand({ hello: 1 });
      print(hello.setName && hello.primary ? 1 : 0);
    } catch (e) {
      print(0);
    }
  ' | grep -q '^1$'; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for authenticated replica set access to ${label}" >&2
      exit 1
    fi
    sleep 2
  done

  echo "${label} accepts authenticated connections and has a PRIMARY."
}

wait_for_mongos_auth() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  echo "Waiting for authenticated mongos..."

  until mongosh "mongodb://mongos:27017/admin" \
    -u "$BOOTSTRAP_USER" \
    -p "$MONGO_BOOTSTRAP_PASSWORD" \
    --authenticationDatabase admin \
    --quiet \
    --eval '
      try {
        print(db.adminCommand({ ping: 1 }).ok);
      } catch (e) {
        print(0);
      }
    ' | grep -q '^1$'; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for authenticated mongos" >&2
      exit 1
    fi
    sleep 2
  done

  echo "mongos is reachable with authentication."
}

CFG_URI="mongodb://cfg1:27019,cfg2:27019,cfg3:27019/admin?replicaSet=cfgRS"
RS0_URI="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/admin?replicaSet=rs0"
RS1_URI="mongodb://mongo4:27017,mongo5:27017,mongo6:27017/admin?replicaSet=rs1"
RS2_URI="mongodb://mongo7:27017,mongo8:27017,mongo9:27017/admin?replicaSet=rs2"

wait_for_rs_auth "cfgRS" "$CFG_URI"
wait_for_rs_auth "rs0" "$RS0_URI"
wait_for_rs_auth "rs1" "$RS1_URI"
wait_for_rs_auth "rs2" "$RS2_URI"

wait_for_mongos_auth

echo "Running cluster initialization via mongos..."

MONGO_CLUSTER_ADMIN_PASSWORD="$MONGO_CLUSTER_ADMIN_PASSWORD" \
MONGO_APP_PASSWORD="$MONGO_APP_PASSWORD" \
mongosh "mongodb://mongos:27017/admin" \
  -u "$BOOTSTRAP_USER" \
  -p "$MONGO_BOOTSTRAP_PASSWORD" \
  --authenticationDatabase admin \
  /scripts/init-cluster.js

echo "Cluster bootstrap finished successfully."
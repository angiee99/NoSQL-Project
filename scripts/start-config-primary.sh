#!/bin/bash
set -e

mongod \
  --configsvr \
  --replSet "${MONGO_REPLSET_NAME}" \
  --bind_ip_all \
  --port "${MONGO_PORT}" \
  --keyFile /run/secrets/mongo_key \
  --fork \
  --logpath /var/log/mongodb.log

until mongosh --host localhost --port "${MONGO_PORT}" \
  --eval "db.adminCommand({ ping: 1 })" --quiet >/dev/null 2>&1; do
  sleep 2
done

MONGO_BOOTSTRAP_PASSWORD="${MONGO_BOOTSTRAP_PASSWORD}" \
mongosh --host localhost --port "${MONGO_PORT}" "${MONGO_INIT_SCRIPT}"

tail -f /var/log/mongodb.log
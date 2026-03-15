#!/bin/bash
set -e

KEY_DIR=/keyfile
KEY_FILE=$KEY_DIR/mongo-keyfile

mkdir -p "$KEY_DIR"

if [ ! -f "$KEY_FILE" ]; then
  echo "Generating MongoDB internal authentication keyfile..."
  openssl rand -base64 756 | tr -d '\n' > "$KEY_FILE"
else
  echo "MongoDB keyfile already exists, reusing it."
fi
# # Make the file readable by the MongoDB process user
chown 999:999 "$KEY_FILE"

# MongoDB requires no group/world permissions on Unix
chmod 600 "$KEY_FILE"

echo "Keyfile ready at $KEY_FILE"
ls -l "$KEY_FILE"
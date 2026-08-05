#!/usr/bin/env bash
set -e
CONFIG_PATH=/data/options.json
PAIRING_CODE=$(jq -r '.pairing_code' $CONFIG_PATH)
AINA_SERVER=$(jq -r '.aina_server' $CONFIG_PATH)
export PAIRING_CODE AINA_SERVER
exec python3 /app/main.py

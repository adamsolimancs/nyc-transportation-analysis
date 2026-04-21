#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLASS_DIR="$SCRIPT_DIR/ingest_class_files"
APP_JAR="$SCRIPT_DIR/taxi_ingest_classes.jar"
MAIN_CLASS="taxi_ingest"

INPUT_PATH="${1:-/user/aes10130_nyu_edu/final_project/yellow_taxi_raw}"
OUTPUT_PATH="${2:-hdfs://nyu-dataproc-m/user/$(whoami)/final_project/processed_taxi}"
LOOKUP_PATH="${3:-hdfs://nyu-dataproc-m/user/$(whoami)/final_project/taxi_zone_lookup.csv}"
PREVIEW_ROWS="${4:-0}"

if [[ ! -d "$CLASS_DIR" ]]; then
  echo "Missing class directory: $CLASS_DIR" >&2
  exit 1
fi

rm -f "$APP_JAR"
jar cf "$APP_JAR" -C "$CLASS_DIR" .

spark-submit \
  --class "$MAIN_CLASS" \
  "$APP_JAR" \
  "$INPUT_PATH" \
  "$OUTPUT_PATH" \
  "$LOOKUP_PATH" \
  "$PREVIEW_ROWS"

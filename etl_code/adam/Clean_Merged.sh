#!/usr/bin/env bash
# this script runs Clean_Merged.scala job from start to finish (compile to spark-submit)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALA_FILE="$SCRIPT_DIR/Clean_Merged.scala"
CLASS_DIR="$SCRIPT_DIR/clean_merged_class_files"
APP_JAR="$SCRIPT_DIR/Clean_Merged_classes.jar"
MAIN_CLASS="Clean_Merged"

INPUT_PATH="${1:-hdfs:///user/aes10130_nyu_edu/final_project/merged_data}"
OUTPUT_PATH="${2:-hdfs:///user/aes10130_nyu_edu/final_project/merged_data}"
TEMP_PATH="${3:-${OUTPUT_PATH}_tmp}"

if ! command -v scalac >/dev/null 2>&1; then
  echo "Missing required command: scalac" >&2
  exit 1
fi

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "Missing required command: spark-submit" >&2
  exit 1
fi

if [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_CLASSPATH="$SPARK_HOME/jars/*"
else
  SPARK_SUBMIT_DIR="$(cd "$(dirname "$(command -v spark-submit)")" && pwd)"
  SPARK_CLASSPATH="$SPARK_SUBMIT_DIR/../jars/*"
fi

mkdir -p "$CLASS_DIR"
rm -rf "$CLASS_DIR"/*

scalac \
  -classpath "$SPARK_CLASSPATH" \
  -d "$CLASS_DIR" \
  "$SCALA_FILE"

rm -f "$APP_JAR"
jar cf "$APP_JAR" -C "$CLASS_DIR" .

spark-submit \
  --class "$MAIN_CLASS" \
  "$APP_JAR" \
  "$INPUT_PATH" \
  "$OUTPUT_PATH" \
  "$TEMP_PATH"

#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALA_FILE="$SCRIPT_DIR/Profiling.scala"
CLASS_DIR="$SCRIPT_DIR/profiling_class_files"
APP_JAR="$SCRIPT_DIR/Profiling_classes.jar"
MAIN_CLASS="Profiling"

INPUT_PATH="${1:-hdfs:///user/aes10130_nyu_edu/final_project/merged_data}"

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
  "$INPUT_PATH"

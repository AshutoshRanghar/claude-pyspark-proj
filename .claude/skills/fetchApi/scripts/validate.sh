#!/usr/bin/env bash
# validate.sh — Validates the latest fetchApi run
# Usage: bash .claude/skills/fetchApi/scripts/validate.sh

set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$SKILL_DIR/data"
LOG_DIR="$SKILL_DIR/logs"

EXPECTED_FILES=(
  "dim_customer.csv"
  "dim_store.csv"
  "dim_date.csv"
  "dim_product.csv"
  "fact_sales.csv"
  "fact_returns.csv"
)

echo "=== fetchApi Validation ==="
echo ""

# --- Find latest data run ---
LATEST_DATA=$(ls -1t "$DATA_DIR" 2>/dev/null | head -1)
if [[ -z "$LATEST_DATA" ]]; then
  echo "ERROR: No data directories found in $DATA_DIR"
  exit 1
fi

echo "Latest run: $LATEST_DATA"
echo "Data dir:   $DATA_DIR/$LATEST_DATA"
echo ""

# --- Check each expected CSV ---
ALL_OK=true
echo "File checks:"
for file in "${EXPECTED_FILES[@]}"; do
  FILEPATH="$DATA_DIR/$LATEST_DATA/$file"
  if [[ -f "$FILEPATH" ]]; then
    SIZE=$(wc -c < "$FILEPATH")
    if [[ "$SIZE" -gt 0 ]]; then
      echo "  [OK] $file ($SIZE bytes)"
    else
      echo "  [WARN] $file exists but is empty"
      ALL_OK=false
    fi
  else
    echo "  [MISSING] $file"
    ALL_OK=false
  fi
done

echo ""

# --- Check log file ---
LATEST_LOG=$(ls -1t "$LOG_DIR" 2>/dev/null | head -1)
if [[ -z "$LATEST_LOG" ]]; then
  echo "WARN: No log directories found in $LOG_DIR"
else
  LOG_FILE="$LOG_DIR/$LATEST_LOG/fetchAPI.log"
  if [[ -f "$LOG_FILE" ]]; then
    ERRORS=$(grep -c "\[ERROR\]" "$LOG_FILE" || true)
    SUCCESSES=$(grep -c "SUCCESS" "$LOG_FILE" || true)
    echo "Log check:  $LOG_FILE"
    echo "  Successes: $SUCCESSES"
    echo "  Errors:    $ERRORS"
    if [[ "$ERRORS" -gt 0 ]]; then
      echo ""
      echo "Error details:"
      grep "\[ERROR\]" "$LOG_FILE" | sed 's/^/  /'
      ALL_OK=false
    fi
  else
    echo "WARN: Log file not found at $LOG_FILE"
  fi
fi

echo ""
if $ALL_OK; then
  echo "Result: PASSED — all files present and non-empty, no errors in log."
  exit 0
else
  echo "Result: FAILED — see issues above."
  exit 1
fi

#!/bin/bash
# Databricks init script to install the wheel package from DBFS

set -e

echo "[INFO] Starting wheel installation..."

# Find and install the latest wheel from DBFS
WHEEL_PATH=$(dbfs ls /libs 2>/dev/null | grep "claude_pyspark_proj.*\.whl" | head -1 | awk '{print $NF}')

if [ -z "$WHEEL_PATH" ]; then
    echo "[WARN] No wheel found in dbfs:/libs/"
    echo "[INFO] Jobs will use workspace code or installed package"
    exit 0
fi

echo "[INFO] Found wheel: $WHEEL_PATH"

# Install the wheel
python -m pip install --upgrade "dbfs:$WHEEL_PATH"

if [ $? -eq 0 ]; then
    echo "[INFO] Successfully installed wheel from $WHEEL_PATH"
else
    echo "[WARN] Failed to install wheel, continuing anyway..."
    exit 0
fi

#!/bin/bash
# Dyness Battery Monitoring Script
# Runs candump → decode → influxdb pipeline

set -e

# Configuration
CAN_INTERFACE="can0"
CANDUMP_DURATION=14
RAM_DIR="/dev/shm/dyness"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CANDUMP_FILE="${RAM_DIR}/candump_${TIMESTAMP}.log"
CELLS_CSV="${RAM_DIR}/dyness_cells_${TIMESTAMP}.csv"
TEMPS_CSV="${RAM_DIR}/dyness_temps_${TIMESTAMP}.csv"
PILE_CSV="${RAM_DIR}/dyness_pile_${TIMESTAMP}.csv"
DECODE_SCRIPT="/home/thomas/decode_dyness.py"
IMPORT_SCRIPT="/home/thomas/import_dyness_to_influx.py"
LOG_FILE="/var/log/dyness_monitor.log"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Create RAM directory if needed
mkdir -p "$RAM_DIR"

log "=== Starting Dyness monitoring cycle ==="

# Step 1: Capture CAN data (14 seconds) with absolute timestamps
log "Step 1: Recording CAN data for ${CANDUMP_DURATION}s..."
cd "$RAM_DIR"
timeout ${CANDUMP_DURATION}s candump -t a ${CAN_INTERFACE} > "$CANDUMP_FILE" 2>&1 || true

# Check if file was created and has content
if [ ! -s "$CANDUMP_FILE" ]; then
    log "ERROR: candump file is empty or not created"
    exit 1
fi

FILE_SIZE=$(stat -c "%s" "$CANDUMP_FILE" 2>/dev/null)
FRAME_COUNT=$(wc -l < "$CANDUMP_FILE")
log "  Captured: ${FRAME_COUNT} frames (${FILE_SIZE} bytes)"

# Step 2: Decode to CSV
log "Step 2: Decoding CAN data to CSV..."
python3 "$DECODE_SCRIPT" "$CANDUMP_FILE" \
    --csv "$CELLS_CSV" \
    --csv-temps "$TEMPS_CSV" \
    --csv-pile "$PILE_CSV" \
    -o /dev/null 2>&1 | grep -E "(loaded|written|cells|temps|pile|Tower)" | head -5 || true

# Check CSV files
if [ -f "$CELLS_CSV" ]; then
    CELL_COUNT=$(($(wc -l < "$CELLS_CSV") - 1))
    log "  Cells CSV: ${CELL_COUNT} cells"
else
    log "WARNING: Cells CSV not created"
fi

if [ -f "$TEMPS_CSV" ]; then
    TEMP_COUNT=$(($(wc -l < "$TEMPS_CSV") - 1))
    log "  Temps CSV: ${TEMP_COUNT} readings"
else
    log "WARNING: Temps CSV not created"
fi

if [ -f "$PILE_CSV" ]; then
    PILE_COUNT=$(($(wc -l < "$PILE_CSV") - 1))
    log "  Pile CSV: ${PILE_COUNT} records"
else
    log "WARNING: Pile CSV not created"
fi

# Step 3: Import to InfluxDB
log "Step 3: Importing to InfluxDB..."
if [ -f "$CELLS_CSV" ] && [ -f "$TEMPS_CSV" ]; then
    if [ -f "$PILE_CSV" ]; then
        python3 "$IMPORT_SCRIPT" "$CELLS_CSV" "$TEMPS_CSV" "$PILE_CSV" 2>&1 | \
            grep -E "(imported|Total|Error|failed)" || true
    else
        python3 "$IMPORT_SCRIPT" "$CELLS_CSV" "$TEMPS_CSV" 2>&1 | \
            grep -E "(imported|Total|Error|failed)" || true
    fi
    log "  Import completed"
else
    log "ERROR: CSV files missing, skipping import"
fi

# Step 4: Cleanup old files (keep last 5 cycles)
log "Step 4: Cleanup..."
cd "$RAM_DIR"
ls -t candump_*.log 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
ls -t dyness_*.csv 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

REMAINING_FILES=$(ls -1 "$RAM_DIR" 2>/dev/null | wc -l)
log "  Remaining files in RAM: ${REMAINING_FILES}"

log "=== Cycle completed successfully ==="
echo ""

exit 0

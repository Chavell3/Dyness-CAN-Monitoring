#!/usr/bin/env python3
"""
Dyness BMS CAN-Bus Decoder
Supports: Dyness Tower T17 (2x in parallel, 5 modules x 30 cells each)
Protocol: Dyness v1.20 (official 0x42xx) + internal BMCU (0x18FF9xxx)

Usage:
    python3 decode_dyness.py [candump_file] [-o output_file]
    python3 decode_dyness.py candump -o dyness_report.txt
"""

import sys
import argparse
import re
from collections import defaultdict
from datetime import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
## change number of towers for your needs (with combiner box multiple towers are supported)
NUM_TOWERS   = 2
## change number of modules for your needs 
NUM_MODULES  = 5
## depending on Dyness model, also change number of Cell per module
CELLS_MODULE = 30
TOTAL_CELLS  = NUM_MODULES * CELLS_MODULE   # 150 per tower

# Internal BMCU multi-frame: cell_mV = LE16 - module_index * 2048
MODULE_MV_OFFSET = 2048

# Temperature single-byte encoding: T = raw - 40
TEMP_OFFSET = 40
TEMP_INVALID = 0xFF

# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------
def parse_candump(path):
    """
    Parse candump log file. Handles two common formats:
      Format A (candump -l):  (timestamp)  canX  XXXXXXXX#BBBBBBBB
      Format B (candump):     canX  XXXXXXXX  [N]  BB BB BB ...
    Returns list of (timestamp, can_id_str_upper, bytes_list)
    """
    frames = []
    fmt_b_re = re.compile(r'^\S+\s+([0-9A-Fa-f]+)\s+\[\d+\]\s+((?:[0-9A-Fa-f]{2}\s*)+)$')
    fmt_a_re = re.compile(r'\(\S+\)\s+\S+\s+([0-9A-Fa-f]+)#([0-9A-Fa-f]+)')
    timestamp_re = re.compile(r'^\s*\(([0-9]+\.[0-9]+)\)')

    with open(path, 'r', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Extract timestamp (format: (1773388211.879699) )
            timestamp = None
            ts_match = timestamp_re.match(line)
            if ts_match:
                timestamp = float(ts_match.group(1))
                # Skip first 22 characters (timestamp format)
                if len(line) > 22:
                    line = line[22:]

            m = fmt_b_re.match(line)
            if m:
                can_id = m.group(1).upper().zfill(8)
                data   = [int(x, 16) for x in m.group(2).split()]
                frames.append((timestamp, can_id, data))
                continue
            m = fmt_a_re.search(line)
            if m:
                can_id = m.group(1).upper().zfill(8)
                raw    = m.group(2)
                data   = [int(raw[i:i+2], 16) for i in range(0, len(raw), 2)]
                frames.append((timestamp, can_id, data))

    return frames

def group_by_id(frames):
    """Group all frames by CAN ID, keeping timestamps."""
    by_id = defaultdict(list)
    for timestamp, can_id, data in frames:
        by_id[can_id].append((timestamp, data))
    return by_id

def first_frame(by_id, can_id):
    """Return data bytes of first frame with given ID, or None."""
    can_id = can_id.upper().zfill(8)
    lst = by_id.get(can_id)
    if lst:
        timestamp, data = lst[0]
        return data
    return None

# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------
def le16(data, offset):
    return data[offset] | (data[offset + 1] << 8)

def ascii_str(data):
    return ''.join(chr(b) if 32 <= b <= 126 else '' for b in data).strip()

def bit_flags(value, bit_map):
    return [label for bit, label in sorted(bit_map.items()) if value & (1 << bit)]

# ---------------------------------------------------------------------------
# Multi-frame reassembly
# ---------------------------------------------------------------------------
def build_seq_bucket(frames_list):
    """
    For a list of 8-byte frames using the Dyness multi-frame format:
      byte[0] = seq, bytes[1-6] = payload, byte[7] = 0xFF - seq
    Returns dict: seq -> (timestamp, data bytes) (one representative per seq number).
    """
    bucket = {}
    for timestamp, data in frames_list:
        if len(data) < 8:
            continue
        seq      = data[0]
        checksum = data[7] & 0xFF
        expected = (0xFF - seq) & 0xFF
        if checksum != expected:
            continue
        if seq not in bucket:
            bucket[seq] = (timestamp, data)
    return bucket

def reassemble_payload(bucket, seq_start, seq_end):
    """Concatenate payload bytes[1-6] for seq_start..seq_end."""
    payload = []
    for seq in range(seq_start, seq_end + 1):
        if seq in bucket:
            timestamp, data = bucket[seq]
            payload.extend(data[1:7])
        else:
            payload.extend([None] * 6)   # mark gaps
    return payload

# ---------------------------------------------------------------------------
# Official protocol decoders  (0x42xx)
# ---------------------------------------------------------------------------
BATTERY_TYPES = {
    1: 'Hpowercube 10.0 / GTI-10',
    2: 'Hpowercube 16.0 / GTI-16',
    3: 'Tower T7',
    4: 'Tower T10',
    5: 'Tower T14',
    6: 'Tower T17',
    7: 'Tower T21',
}

FAULT_BITS = {
    0: 'Voltage sensor error',
    1: 'Temperature sensor error',
    2: 'Internal comm error',
    3: 'Input over-voltage',
    4: 'Input reverse polarity',
    5: 'Relay check error',
    6: 'Battery cell error (over-discharge)',
    7: 'Other error (see ext fault)',
}

ALARM_BITS = {
    0:  'Single cell low voltage',
    1:  'Single cell high voltage',
    2:  'System discharge low voltage',
    3:  'System charge high voltage',
    4:  'Charge cell low temperature',
    5:  'Charge cell high temperature',
    6:  'Discharge cell low temperature',
    7:  'Discharge cell high temperature',
    8:  'Charge over-current',
    9:  'Discharge over-current',
    10: 'Module low voltage',
    11: 'Module high voltage',
}

PROT_BITS = {
    0:  'Single cell under-voltage',
    1:  'Single cell over-voltage',
    2:  'System discharge under-voltage',
    3:  'System charge over-voltage',
    4:  'Charge under-temperature',
    5:  'Charge over-temperature',
    6:  'Discharge under-temperature',
    7:  'Discharge over-temperature',
    8:  'Charge over-current',
    9:  'Discharge over-current',
    10: 'Module under-voltage',
    11: 'Module over-voltage',
    12: 'Module 2nd-level under-voltage',
}

FAULT_EXT_BITS = {
    0: 'Shutdown circuit error',
    1: 'BMIC error',
    2: 'Internal bus error',
    3: 'Self-test error',
}

STATE_MAP = {0: 'Sleep', 1: 'Charging', 2: 'Discharging', 3: 'Idle'}

def decode_official(by_id):
    results = {}

    d = first_frame(by_id, '000042F0')
    if d:
        results['manufacturer'] = ascii_str(d) or '(empty)'

    d = first_frame(by_id, '00004300')
    if d:
        t = d[0]
        results['battery_type_raw'] = t
        results['battery_type'] = BATTERY_TYPES.get(t, f'Unknown (0x{t:02X})')

    d = first_frame(by_id, '000042E0')
    if d:
        results['serial'] = ascii_str(d) or '(not set)'

    d = first_frame(by_id, '00004210')
    if d and len(d) >= 8:
        results['voltage_V']   = le16(d, 0) * 0.1
        results['current_A']   = le16(d, 2) * 0.1 - 3000.0
        results['bms_temp_C']  = le16(d, 4) * 0.1 - 100.0
        results['soc_pct']     = d[6]
        results['soh_pct']     = d[7]

    d = first_frame(by_id, '00004220')
    if d and len(d) >= 8:
        results['chg_cutoff_V']  = le16(d, 0) * 0.1
        results['dis_cutoff_V']  = le16(d, 2) * 0.1
        results['max_chg_A']     = le16(d, 4) * 0.1 - 3000.0
        results['max_dis_A']     = abs(le16(d, 6) * 0.1 - 3000.0)

    d = first_frame(by_id, '00004230')
    if d and len(d) >= 8:
        results['cell_max_V']    = le16(d, 0) * 0.001
        results['cell_min_V']    = le16(d, 2) * 0.001
        results['cell_max_num']  = le16(d, 4)
        results['cell_min_num']  = le16(d, 6)

    d = first_frame(by_id, '00004240')
    if d and len(d) >= 8:
        results['cell_max_T'] = le16(d, 0) * 0.1 - 100.0
        results['cell_min_T'] = le16(d, 2) * 0.1 - 100.0

    d = first_frame(by_id, '00004250')
    if d and len(d) >= 8:
        status = d[0]
        results['state']         = STATE_MAP.get(status & 0x07, f'Unknown ({status & 7})')
        results['forced_charge'] = bool(status & 0x08)
        results['bal_charge']    = bool(status & 0x10)
        results['cycle_count']   = d[1]
        fault = d[2] | (d[3] << 8)
        alarm = d[4] | (d[5] << 8)
        prot  = d[6] | (d[7] << 8)
        results['faults']        = bit_flags(fault, FAULT_BITS)
        results['alarms']        = bit_flags(alarm, ALARM_BITS)
        results['protections']   = bit_flags(prot,  PROT_BITS)

    d = first_frame(by_id, '00004280')
    if d and len(d) >= 2:
        results['chg_forbidden'] = d[0] == 0xAA
        results['dis_forbidden'] = d[1] == 0xAA

    d = first_frame(by_id, '00004290')
    if d:
        results['fault_ext'] = bit_flags(d[0], FAULT_EXT_BITS)

    return results

# ---------------------------------------------------------------------------
# Internal BMCU cell voltage decoder  (0x18FF97xx)
# ---------------------------------------------------------------------------
def decode_cell_voltages(by_id, tower_sa):
    """
    tower_sa: 1 or 2  (CAN SA byte = 0x01 or 0x02)
    Returns: list of 150 cell voltages (mV) or None per cell if missing.
    Modules 1-5 = seq blocks 0-29, 30-59, ..., 120-148.
    cell_mV = LE16(bytes[1:3]) - module_index * 2048
    """
    can_id = f'18FF97{tower_sa:02X}'
    frames = by_id.get(can_id.upper())
    if not frames:
        return None, {}, None

    bucket = build_seq_bucket(frames)
    max_seq = max(bucket.keys()) if bucket else 0

    # Get representative timestamp (first frame)
    first_timestamp = None
    if bucket and 0 in bucket:
        first_timestamp, _ = bucket[0]
    elif bucket:
        first_timestamp, _ = bucket[min(bucket.keys())]

    stats = {'id': can_id, 'captured': len(bucket), 'max_seq': max_seq}

    cells = []   # list of 150 entries, each = mV or None
    for mod_idx in range(NUM_MODULES):
        seq_start = mod_idx * 30
        seq_end   = seq_start + 29
        offset_mv = mod_idx * MODULE_MV_OFFSET

        mod_cells = []
        for seq in range(seq_start, seq_end + 1):
            if seq not in bucket:
                continue
            timestamp, data = bucket[seq]
            for b in range(1, 6, 2):
                raw = data[b] | (data[b + 1] << 8)
                mv  = raw - offset_mv
                mod_cells.append(mv if (2000 < mv < 4500) else None)

        # Pad or trim to CELLS_MODULE
        mod_cells = mod_cells[:CELLS_MODULE]
        while len(mod_cells) < CELLS_MODULE:
            mod_cells.append(None)
        cells.extend(mod_cells)

    return cells, stats, first_timestamp

# ---------------------------------------------------------------------------
# Internal BMCU temperature decoder  (0x18FF98xx)
# ---------------------------------------------------------------------------
def decode_temperatures(by_id, tower_sa):
    """
    Single-byte temperature: T_degC = raw - 40. 0xFF = not populated.
    Returns flat list of valid temps with their position index and timestamp.
    """
    can_id = f'18FF98{tower_sa:02X}'
    frames = by_id.get(can_id.upper())
    if not frames:
        return [], {}

    bucket = build_seq_bucket(frames)
    stats  = {'id': can_id, 'captured': len(bucket)}

    temps = []
    for seq in sorted(bucket.keys()):
        timestamp, data = bucket[seq]
        for b in range(1, 7):
            raw = data[b]
            if raw != TEMP_INVALID:
                temps.append({'seq': seq, 'raw': raw, 'degC': raw - TEMP_OFFSET, 'timestamp': timestamp})

    return temps, stats

# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------
DIVIDER  = '=' * 72
DIVIDER2 = '-' * 72

def fmt_flag(val, yes='YES', no='No'):
    return yes if val else no

def fmt_list(items, empty='None'):
    if not items:
        return f'  {empty}'
    return '\n'.join(f'  *** {x}' for x in items)

def write_report(official, tower_cells, tower_temps, by_id, output_file):
    lines = []
    def w(*args):
        lines.append(' '.join(str(a) for a in args))

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    w(DIVIDER)
    w(' DYNESS BMS FULL DECODE REPORT')
    w(f' Generated: {now}')
    w(DIVIDER)

    # ---- Pack summary ----
    w()
    w('SYSTEM IDENTIFICATION')
    w(DIVIDER2)
    w(f"  Manufacturer : {official.get('manufacturer', 'N/A')}")
    w(f"  Battery Type : {official.get('battery_type', 'N/A')}")
    w(f"  Serial Number: {official.get('serial', 'N/A')}")
    w(f"  Configuration: 2 x Tower T17 in parallel")
    w(f"                 2 towers x 5 modules x 30 cells = 300 cells total")

    w()
    w('PACK STATUS')
    w(DIVIDER2)
    w(f"  State              : {official.get('state', 'N/A')}")
    w(f"  Total Voltage      : {float(official.get("voltage_V", 0) or 0):.1f} V")
    cur = official.get('current_A', None)
    if cur is not None:
        dir_str = '(charging)' if cur > 0.1 else ('(discharging)' if cur < -0.1 else '(idle)')
        w(f"  Current            : {cur:.1f} A  {dir_str}")
    w(f"  BMS Temperature    : {float(official.get("bms_temp_C", 0) or 0):.1f} degC")
    w(f"  SOC                : {official.get('soc_pct', 'N/A')} %")
    w(f"  SOH                : {official.get('soh_pct', 'N/A')} %")
    w(f"  Cycle Count        : {official.get('cycle_count', 'N/A')}")
    w(f"  Forced Charge Req  : {fmt_flag(official.get('forced_charge', False))}")
    w(f"  Balance Charge Req : {fmt_flag(official.get('bal_charge', False))}")
    w(f"  Charge Forbidden   : {fmt_flag(official.get('chg_forbidden', False))}")
    w(f"  Discharge Forbidden: {fmt_flag(official.get('dis_forbidden', False))}")

    w()
    w('CHARGE / DISCHARGE LIMITS')
    w(DIVIDER2)
    w(f"  Charge Cutoff Voltage   : {float(official.get("chg_cutoff_V", 0) or 0):.1f} V")
    w(f"  Discharge Cutoff Voltage: {float(official.get("dis_cutoff_V", 0) or 0):.1f} V")
    w(f"  Max Charge Current      : {float(official.get("max_chg_A", 0) or 0):.1f} A")
    w(f"  Max Discharge Current   : {float(official.get("max_dis_A", 0) or 0):.1f} A")

    w()
    w('CELL EXTREMES  (pack-wide, from official 0x4230/0x4240 frames)')
    w(DIVIDER2)
    cmax = official.get('cell_max_V', None)
    cmin = official.get('cell_min_V', None)
    if cmax and cmin:
        w(f"  Max Cell Voltage: {cmax:.3f} V  (Cell #{official.get('cell_max_num', '?')})")
        w(f"  Min Cell Voltage: {cmin:.3f} V  (Cell #{official.get('cell_min_num', '?')})")
        w(f"  Voltage Spread  : {(cmax - cmin) * 1000:.0f} mV")
    w(f"  Max Cell Temp   : {float(official.get("cell_max_T", 0) or 0):.1f} degC")
    w(f"  Min Cell Temp   : {float(official.get("cell_min_T", 0) or 0):.1f} degC")

    w()
    w('ALARMS / FAULTS / PROTECTION')
    w(DIVIDER2)
    for key, label in [('faults', 'Faults'), ('alarms', 'Alarms'), ('protections', 'Protection')]:
        items = official.get(key, [])
        if items:
            w(f"  {label}:")
            for x in items:
                w(f"    *** {x}")
        else:
            w(f"  {label}: None")
    ext = official.get('fault_ext', [])
    if ext:
        w("  Extended Faults:")
        for x in ext:
            w(f"    *** {x}")
    else:
        w("  Extended Faults: None")

    # ---- Per-tower per-cell voltages ----
    w()
    w(DIVIDER)
    w(' PER-CELL VOLTAGES  (internal BMCU 0x18FF97xx)')
    w(' Encoding: cell_mV = LE16 - module_index x 2048')
    w(DIVIDER)

    for tower_idx, (cells, stats, timestamp) in enumerate(tower_cells):
        tower_num = tower_idx + 1
        w()
        w(f'TOWER {tower_num}  (CAN SA=0x{tower_num:02X}, ID {stats.get("id","?")})')
        w(f'  Frames captured: {stats.get("captured", 0)} of {stats.get("max_seq", 0)+1}')
        w(DIVIDER2)

        if not cells:
            w('  No data.')
            continue

        valid_cells = [v for v in cells if v is not None]
        if valid_cells:
            pack_min = min(valid_cells)
            pack_max = max(valid_cells)
            w(f'  Pack range: {pack_min/1000:.3f} V  to  {pack_max/1000:.3f} V  (spread {pack_max - pack_min} mV)')

        for mod_idx in range(NUM_MODULES):
            start = mod_idx * CELLS_MODULE
            end   = start + CELLS_MODULE
            mod_cells = cells[start:end]
            valid = [v for v in mod_cells if v is not None]

            w()
            if valid:
                mn = min(valid)
                mx = max(valid)
                avg = sum(valid) / len(valid)
                w(f'  Module {mod_idx+1}  ({len(valid)} cells decoded)')
                w(f'  {"Cell":>6}  {"Voltage":>10}  {"Delta":>8}')
                w(f'  {"----":>6}  {"-------":>10}  {"-----":>8}')
                for cell_idx, mv in enumerate(mod_cells):
                    cell_num = cell_idx + 1
                    if mv is not None:
                        delta = mv - round(avg)
                        flag  = ' <-- LOW' if mv == mn and (mx - mn) > 5 else ''
                        flag  = ' <-- HIGH' if mv == mx and (mx - mn) > 5 else flag
                        w(f'  {cell_num:>6}  {mv/1000:>10.3f} V  {delta:>+7} mV{flag}')
                    else:
                        w(f'  {cell_num:>6}  {"(no data)":>10}')
                w()
                w(f'  Module {mod_idx+1} Summary:')
                w(f'    Min: {mn/1000:.3f} V   Max: {mx/1000:.3f} V   Avg: {avg/1000:.3f} V   Spread: {mx-mn} mV')
            else:
                w(f'  Module {mod_idx+1}: no data decoded')

    # ---- Temperatures ----
    w()
    w(DIVIDER)
    w(' CELL TEMPERATURES  (internal BMCU 0x18FF98xx)')
    w(' Encoding: T_degC = raw_byte - 40')
    w(DIVIDER)

    for tower_idx, (temps, stats) in enumerate(tower_temps):
        tower_num = tower_idx + 1
        w()
        w(f'TOWER {tower_num}  (CAN SA=0x{tower_num:02X}, ID {stats.get("id","?")})')
        w(f'  Frames captured: {stats.get("captured", 0)}')
        w(DIVIDER2)

        if not temps:
            w('  No temperature data.')
            continue

        all_t = [t['degC'] for t in temps]
        w(f'  Range: {min(all_t)} degC  to  {max(all_t)} degC')
        w()
        w(f'  {"#":>4}  {"Seq":>5}  {"Raw":>5}  {"Temp":>8}')
        w(f'  {"--":>4}  {"---":>5}  {"---":>5}  {"----":>8}')
        for i, t in enumerate(temps):
            w(f'  {i+1:>4}  {t["seq"]:>5}  {t["raw"]:>5} (0x{t["raw"]:02X})  {t["degC"]:>6} degC')

    # ---- CAN ID inventory ----
    w()
    w(DIVIDER)
    w(' CAN ID INVENTORY')
    w(DIVIDER)
    w(f'  {"CAN ID":>12}  {"Frames":>8}  Description')
    w(f'  {"------":>12}  {"------":>8}  -----------')

    id_descriptions = {
        '00004200': 'Host query (broadcast)',
        '00004210': 'Ensemble: voltage / current / temp / SOC / SOH',
        '00004220': 'Charge / discharge cutoff limits',
        '00004230': 'Cell voltage extremes (max/min + cell#)',
        '00004240': 'Cell temperature extremes',
        '00004250': 'Status / faults / alarms / protection',
        '00004260': 'Module voltage extremes',
        '00004270': 'Module temperature extremes',
        '00004280': 'Charge / discharge forbidden flags',
        '00004290': 'Extended fault register',
        '000042E0': 'Serial number (ASCII)',
        '000042F0': 'Manufacturer name (ASCII)',
        '00004300': 'Battery type',
        '18FF9701': 'Tower 1 per-cell voltages (multi-frame, 149 sub-frames)',
        '18FF9702': 'Tower 2 per-cell voltages (multi-frame, 149 sub-frames)',
        '18FF9801': 'Tower 1 per-cell temperatures',
        '18FF9802': 'Tower 2 per-cell temperatures',
        '18FF9A00': 'BMS master summary (SA=0x00)',
        '18FF9A01': 'Module summary Tower 1 (SA=0x01)',
        '18FF9A02': 'Module summary Tower 2 (SA=0x02)',
        '18FF9AD2': 'BMS master multi-frame telemetry (SA=0xD2)',
        '18FF9BD0': 'Host keepalive / charge enable command',
        '18FFA000': 'Unknown internal frame A',
        '18FFA001': 'Unknown internal frame B',
    }

    for can_id in sorted(by_id.keys()):
        count = len(by_id[can_id])
        desc  = id_descriptions.get(can_id, '')
        w(f'  0x{can_id:>10}  {count:>8}  {desc}')

    w()
    w(DIVIDER)
    w(' END OF REPORT')
    w(DIVIDER)

    return '\n'.join(lines)

# ---------------------------------------------------------------------------
# CSV export for spreadsheet use
# ---------------------------------------------------------------------------
def write_csv(tower_cells, output_path):
    rows = []
    rows.append('Timestamp,Tower,Module,Cell,Voltage_V,Voltage_mV,Delta_V')
    for tower_idx, (cells, _, timestamp) in enumerate(tower_cells):
        tower_num = tower_idx + 1
        # Format timestamp for CSV
        timestamp_str = ''
        if timestamp:
            dt = datetime.fromtimestamp(timestamp)
            timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # milliseconds

        for mod_idx in range(NUM_MODULES):
            start = mod_idx * CELLS_MODULE
            mod_c = cells[start:start + CELLS_MODULE]
            valid = [v for v in mod_c if v is not None]
            avg   = sum(valid) / len(valid) if valid else 0
            for cell_idx, mv in enumerate(mod_c):
                if mv is not None:
                    delta_mv = mv - round(avg)
                    delta_v = delta_mv / 1000.0  # Convert mV to V
                    rows.append(f'{timestamp_str},{tower_num},{mod_idx+1},{cell_idx+1},{mv/1000:.3f},{mv},{delta_v:+.4f}')
                # Skip rows with no data (don't write empty cells)
    with open(output_path, 'w') as f:
        f.write('\n'.join(rows) + '\n')

def write_csv_temps(tower_temps, output_path):
    rows = []
    rows.append('Timestamp,Tower,Reading,Seq,Raw_Hex,Raw_Dec,Temp_degC')
    for tower_idx, (temps, _) in enumerate(tower_temps):
        tower_num = tower_idx + 1
        for i, t in enumerate(temps):
            # Format timestamp
            timestamp_str = ''
            if 'timestamp' in t and t['timestamp']:
                dt = datetime.fromtimestamp(t['timestamp'])
                timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            rows.append(f'{timestamp_str},{tower_num},{i+1},{t["seq"]},0x{t["raw"]:02X},{t["raw"]},{t["degC"]}')
    with open(output_path, 'w') as f:
        f.write('\n'.join(rows) + '\n')

def write_csv_battery_pile(by_id, output_path):
    """Export battery pile total info (0x4210) to CSV for InfluxDB import."""
    rows = []
    rows.append('Timestamp,Voltage_V,Current_A,BMS_Temp_C,SOC_pct,SOH_pct')

    # Get all 0x4210 frames (not just first)
    can_id = '00004210'
    frames = by_id.get(can_id, [])

    for timestamp, data in frames:
        if len(data) < 8:
            continue

        # Format timestamp
        timestamp_str = ''
        if timestamp:
            dt = datetime.fromtimestamp(timestamp)
            timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Decode according to protocol (LSB first)
        voltage_V  = ((data[0] | (data[1] << 8)) * 0.1)
        current_A  = ((data[2] | (data[3] << 8)) * 0.1) - 3000.0
        bms_temp_C = ((data[4] | (data[5] << 8)) * 0.1) - 100.0
        soc_pct    = data[6]
        soh_pct    = data[7]

        rows.append(f'{timestamp_str},{voltage_V:.1f},{current_A:.1f},{bms_temp_C:.1f},{soc_pct},{soh_pct}')

    with open(output_path, 'w') as f:
        f.write('\n'.join(rows) + '\n')

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Dyness BMS CAN-bus decoder')
    parser.add_argument('input',  nargs='?', default='candump',
                        help='candump log file (default: candump)')
    parser.add_argument('-o', '--output', default='dyness_report.txt',
                        help='output report file (default: dyness_report.txt)')
    parser.add_argument('--csv', default='dyness_cells.csv',
                        help='CSV output for cell voltages (default: dyness_cells.csv)')
    parser.add_argument('--csv-temps', default='dyness_temps.csv',
                        help='CSV output for temperatures (default: dyness_temps.csv)')
    parser.add_argument('--csv-pile', default='dyness_pile.csv',
                        help='CSV output for battery pile info (default: dyness_pile.csv)')
    args = parser.parse_args()

    print(f'Loading {args.input}...')
    frames = parse_candump(args.input)
    if not frames:
        print('ERROR: no frames parsed. Check file format.')
        sys.exit(1)
    print(f'  {len(frames):,} frames loaded')

    by_id = group_by_id(frames)
    print(f'  {len(by_id)} unique CAN IDs')

    print('Decoding official protocol frames...')
    official = decode_official(by_id)

    print('Decoding per-cell voltages (0x18FF97xx)...')
    tower_cells = []
    for sa in range(1, NUM_TOWERS + 1):
        cells, stats, timestamp = decode_cell_voltages(by_id, sa)
        tower_cells.append((cells or [None] * TOTAL_CELLS, stats, timestamp))

    print('Decoding temperatures (0x18FF98xx)...')
    tower_temps = []
    for sa in range(1, NUM_TOWERS + 1):
        temps, stats = decode_temperatures(by_id, sa)
        tower_temps.append((temps, stats))

    print('Generating report...')
    report = write_report(official, tower_cells, tower_temps, by_id, args.output)

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'Report written to: {args.output}')

    write_csv(tower_cells, args.csv)
    print(f'CSV written to:    {args.csv}')

    write_csv_temps(tower_temps, args.csv_temps)
    print(f'Temp CSV written to: {args.csv_temps}')

    write_csv_battery_pile(by_id, args.csv_pile)
    print(f'Pile CSV written to: {args.csv_pile}')

    # Print compact summary to console
    print()
    print('=' * 60)
    print('  QUICK SUMMARY')
    print('=' * 60)
    print(f"  Manufacturer : {official.get('manufacturer', 'N/A')}")
    print(f"  Type         : {official.get('battery_type', 'N/A')}")
    print(f"  State        : {official.get('state', 'N/A')}")
    print(f"  Voltage      : {official.get('voltage_V', 0):.1f} V")
    print(f"  Current      : {official.get('current_A', 0):.1f} A")
    print(f"  SOC / SOH    : {official.get('soc_pct', '?')} % / {official.get('soh_pct', '?')} %")
    print(f"  Cycles       : {official.get('cycle_count', '?')}")
    print(f"  BMS Temp     : {official.get('bms_temp_C', 0):.1f} degC")
    print()
    for tower_idx, (cells, stats, timestamp) in enumerate(tower_cells):
        valid = [v for v in cells if v is not None]
        if valid:
            mn, mx = min(valid), max(valid)
            print(f"  Tower {tower_idx+1}: {len(valid)} cells  min={mn/1000:.3f}V  max={mx/1000:.3f}V  spread={mx-mn}mV")
        else:
            print(f"  Tower {tower_idx+1}: no cell data")
    print()
    faults = official.get('faults', []) + official.get('alarms', []) + official.get('protections', [])
    if faults:
        print(f"  *** ACTIVE ISSUES: {len(faults)}")
        for f in faults:
            print(f"      - {f}")
    else:
        print("  No faults, alarms, or active protection")
    print('=' * 60)

if __name__ == '__main__':
    main()

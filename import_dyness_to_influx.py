#!/usr/bin/env python3
"""
Import Dyness CSV data to InfluxDB
"""
import csv
import sys
from datetime import datetime
import urllib.request
import urllib.parse

## InfluxDB Configuration // change to your needs
INFLUX_URL = "http://InfluxIP:8086"
INFLUX_ORG = "yourInfluxOrg"
INFLUX_BUCKET = "yourbucket"
INFLUX_TOKEN = "yourInfluxToken"

def parse_timestamp(ts_str):
    """Parse timestamp from CSV format to nanoseconds since epoch"""
    if not ts_str:
        return None
    # Format: 2026-03-13 09:48:27.243
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    return int(dt.timestamp() * 1_000_000_000)  # nanoseconds

def write_to_influx(lines):
    """Write line protocol data to InfluxDB"""
    if not lines:
        return
    
    data = '\n'.join(lines).encode('utf-8')
    url = f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns"
    
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Authorization', f'Token {INFLUX_TOKEN}')
    req.add_header('Content-Type', 'text/plain; charset=utf-8')
    
    try:
        with urllib.request.urlopen(req) as response:
            return response.status == 204
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.read().decode()}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def import_cells(csv_path, batch_size=1000):
    """Import dyness_cells.csv"""
    print(f"Importing cell voltages from {csv_path}...")
    lines = []
    count = 0
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_timestamp(row['Timestamp'])
            if not ts:
                continue
            
            tower = row['Tower']
            module = row['Module']
            cell = row['Cell']
            voltage = row['Voltage_V']
            
            if not voltage:
                continue
            
            # Line protocol: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp
            line = f"dyness_cells,1_tower={tower},2_module={module},3_cell={cell} voltage={voltage} {ts}"
            lines.append(line)
            count += 1
            
            # Write in batches
            if len(lines) >= batch_size:
                if write_to_influx(lines):
                    print(f"  Written {count} cells...")
                    lines = []
                else:
                    print("  Write failed!")
                    return False
    
    # Write remaining
    if lines:
        if write_to_influx(lines):
            print(f"  ✓ Total: {count} cell voltages imported")
            return True
        else:
            print("  Write failed!")
            return False
    
    return True

def import_temps(csv_path, batch_size=1000):
    """Import dyness_temps.csv"""
    print(f"Importing temperatures from {csv_path}...")
    lines = []
    count = 0
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_timestamp(row['Timestamp'])
            if not ts:
                continue
            
            tower = row['Tower']
            reading = row['Reading']
            temp = row['Temp_degC']
            
            if not temp:
                continue
            
            # Line protocol
            line = f"dyness_temps,1_tower={tower},reading={reading} temperature={temp} {ts}"
            lines.append(line)
            count += 1
            
            # Write in batches
            if len(lines) >= batch_size:
                if write_to_influx(lines):
                    print(f"  Written {count} temps...")
                    lines = []
                else:
                    print("  Write failed!")
                    return False
    
    # Write remaining
    if lines:
        if write_to_influx(lines):
            print(f"  ✓ Total: {count} temperatures imported")
            return True
        else:
            print("  Write failed!")
            return False
    
    return True

def import_pile(csv_path, batch_size=1000):
    """Import dyness_pile.csv (battery pile total info from 0x4210)"""
    print(f"Importing battery pile info from {csv_path}...")
    lines = []
    count = 0

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_timestamp(row['Timestamp'])
            if not ts:
                continue

            voltage_v = row['Voltage_V']
            current_a = row['Current_A']
            bms_temp_c = row['BMS_Temp_C']
            soc_pct = row['SOC_pct']
            soh_pct = row['SOH_pct']

            # Line protocol: dyness_pile measurement with fields (no tags needed - single value per timestamp)
            line = f"dyness_pile voltage={voltage_v},current={current_a},bms_temp={bms_temp_c},soc={soc_pct},soh={soh_pct} {ts}"
            lines.append(line)
            count += 1

            # Write in batches
            if len(lines) >= batch_size:
                if write_to_influx(lines):
                    print(f"  Written {count} pile records...")
                    lines = []
                else:
                    print("  Write failed!")
                    return False

    # Write remaining
    if lines:
        if write_to_influx(lines):
            print(f"  ✓ Total: {count} pile records imported")
            return True
        else:
            print("  Write failed!")
            return False

    return True

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 import_dyness_to_influx.py <cells.csv> <temps.csv> [pile.csv]")
        sys.exit(1)
    
    cells_csv = sys.argv[1]
    temps_csv = sys.argv[2]
    pile_csv = sys.argv[3] if len(sys.argv) > 3 else None

    print("=" * 60)
    print("Dyness → InfluxDB Import")
    print("=" * 60)
    print(f"Target: {INFLUX_URL}")
    print(f"Bucket: {INFLUX_BUCKET}")
    print(f"Org:    {INFLUX_ORG}")
    print("=" * 60)
    print()
    
    # Import cells
    if not import_cells(cells_csv):
        print("❌ Cell import failed!")
        sys.exit(1)
    
    print()
    
    # Import temps
    if not import_temps(temps_csv):
        print("❌ Temperature import failed!")
        sys.exit(1)

    print()

    # Import pile info (optional)
    if pile_csv:
        if not import_pile(pile_csv):
            print("❌ Pile import failed!")
            sys.exit(1)
        print()

    print("=" * 60)
    print("✅ Import completed successfully!")
    print("=" * 60)

if __name__ == '__main__':
    main()

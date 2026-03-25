Monitoring-Pipeline

1. dyness_monitor.sh
Pfad: dyness_monitor.sh
Größe: 3.0 KB
Zweck: Orchestriert die komplette Monitoring-Pipeline
Was es tut:
Läuft alle 20 Sekunden (via systemd timer)
Nimmt 14 Sekunden CAN-Daten auf (candump -t a can0)
Ruft decode_dyness.py auf
Ruft import_dyness_to_influx.py auf
Speichert temporäre Dateien in /dev/shm/dyness/ (RAM)
Räumt alte Dateien auf (max. 20 CSV-Dateien)
Loggt alle Schritte nach /var/log/dyness_monitor.log

2. decode_dyness.py
Pfad: decode_dyness.py
Größe: 27 KB
Zweck: Parsed CAN-Bus-Daten der Dyness Tower T7 Batterie
Was es tut:
Liest candump-Logs (unterstützt beide Formate)
Decodiert Cell-Voltages (CAN-ID 0x100-0x11F) für 2 Towers × 5 Module × 30 Zellen
Decodiert Temperaturen (CAN-ID 0x120-0x13F)
Decodiert Battery Pile Total (CAN-ID 0x4210): Voltage, Current, BMS-Temp, SOC, SOH
Konvertiert LSB-First-Encoding mit Offsets
Output:
dyness_cells.csv: Timestamp, Tower, Module, Cell, Voltage_V, Voltage_mV, Delta_V
dyness_temps.csv: Timestamp, Tower, Reading, Temp_degC
dyness_pile.csv: Timestamp, Voltage_V, Current_A, BMS_Temp_C, SOC_pct, SOH_pct

3. import_dyness_to_influx.py
Pfad: import_dyness_to_influx.py
Größe: 6.6 KB
Zweck: Importiert CSV-Daten in InfluxDB v2.x
Was es tut:
Verbindet zu InfluxDB (Influx-Server:8086, Bucket: Influx_bucket)
Importiert dyness_cells.csv → Measurement: dyness_cells
Importiert dyness_temps.csv → Measurement: dyness_temps
Importiert dyness_pile.csv → Measurement: dyness_pile
Batch-Import mit 1000 Datenpunkten pro Request
Verwendet Line Protocol Format

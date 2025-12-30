#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VR-TS1 Bonepile Statistics Report
- Valid SN: Format 183XXXXXXXX (13 digits, starts with 183)
- Fail: PIC = IGS and result = FAIL
- Pass: Total unique SN - Fail
- Disposition: Each row with valid SN = 1 disposition (count duplicates)
- Completed disposition: Total disposition - SN Fail with empty IGS Action
- In process: IGS Status contains "waiting", "testing", "in process", "in progress"
- Waiting for material: Contains "waiting for material", "strata", "cx9"
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import io
import warnings
warnings.filterwarnings('ignore')

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

print("=" * 80)
print("VR-TS1 BONEPILE STATISTICS REPORT")
print("=" * 80)

# Read sheet VR-TS1
excel_file = 'NV_IGS_VR144_Bonepile.xlsx'
print("\nReading sheet VR-TS1...")

df = pd.read_excel(excel_file, sheet_name='VR-TS1', header=1)

# Get column names
sn_col = 'sn'
pic_col = 'PIC'
result_col = 'result'
igs_action_col = 'IGS Action '
igs_status_col = 'IGS Status'
bp_duration_col = 'bp_duration'

# Remove duplicate header row if exists
if len(df) > 0 and sn_col in df.columns:
    if str(df.iloc[0][sn_col]).strip() == 'sn':
        df = df.iloc[1:].reset_index(drop=True)

print(f"Total records read: {len(df)}")

# Check columns
print("\nAnalyzing data...")
print("Columns:", list(df.columns))

# Function to check valid SN: starts with 183 and has 13 digits
def is_valid_sn(sn):
    """Check if SN is valid: format 183XXXXXXXX (13 digits, starts with 183)"""
    if pd.isna(sn):
        return False
    # Handle both float and string
    if isinstance(sn, (int, float)):
        sn_str = str(int(sn))  # Convert float to int then to string
    else:
        sn_str = str(sn).strip().replace('.0', '')  # Handle "1830125000128.0"
    
    # Check starts with 183 and has exactly 13 digits
    return sn_str.startswith('183') and len(sn_str) == 13 and sn_str.isdigit()

# Filter rows with valid SN
print("\nFiltering rows with valid SN (format 183XXXXXXXX, 13 digits)...")
valid_sn_records = df[df[sn_col].apply(is_valid_sn)]
print(f"Number of rows with valid SN: {len(valid_sn_records)}")

# Count Disposition: Each row with valid SN = 1 disposition (count duplicates)
total_dispositions = len(valid_sn_records)
print(f"\nTotal Dispositions (each row with valid SN = 1): {total_dispositions}")

# Count Fail: PIC = IGS and result = FAIL
print("\nCounting Fail (PIC = IGS and result = FAIL)...")
fail_records = valid_sn_records[
    (valid_sn_records[pic_col].str.upper() == 'IGS') & 
    (valid_sn_records[result_col].str.upper() == 'FAIL')
]
total_fail = len(fail_records)
print(f"Total Fail rows: {total_fail}")

# Get unique SN list
unique_valid_sns = valid_sn_records[sn_col].unique()
total_unique_sns = len(unique_valid_sns)
print(f"\nTotal unique valid SN: {total_unique_sns}")

# Count Fail and Pass (unique SN)
unique_fail_sns = set(fail_records[sn_col].unique())
total_unique_fail = len(unique_fail_sns)

total_unique_pass = total_unique_sns - total_unique_fail
print(f"Unique SN Fail: {total_unique_fail}")
print(f"Unique SN Pass: {total_unique_pass}")
print(f"Total trays: {total_unique_sns} (Fail: {total_unique_fail} + Pass: {total_unique_pass})")

# Count Disposition completed
print("\nCounting completed Dispositions...")
# In total Fail, check which SN has empty IGS Action
fail_with_empty_action = fail_records[
    (fail_records[igs_action_col].isna()) | 
    (fail_records[igs_action_col].astype(str).str.strip() == '') |
    (fail_records[igs_action_col].astype(str).str.strip() == 'nan')
]

# Count unique SN fail with empty IGS Action
unique_fail_empty_action = set(fail_with_empty_action[sn_col].unique())
num_fail_empty_action = len(unique_fail_empty_action)

# Disposition completed = Total disposition - SN Fail with empty IGS Action
dispositions_completed = total_dispositions - num_fail_empty_action
print(f"SN Fail with empty IGS Action: {num_fail_empty_action}")
print(f"Completed Dispositions: {dispositions_completed}")

# Check IGS Status for Fail records with IGS Action (not empty)
print("\nChecking IGS Status for in-process dispositions...")
fail_with_action = fail_records[~fail_records[sn_col].isin(unique_fail_empty_action)]

# Function to check if status is "in process"
def is_in_process(status):
    """Check if IGS Status indicates in process"""
    if pd.isna(status):
        return False
    status_str = str(status).lower()
    in_process_keywords = ['waiting', 'testing', 'in process', 'in progress']
    return any(keyword in status_str for keyword in in_process_keywords)

# Filter in-process dispositions
in_process_records = fail_with_action[
    fail_with_action[igs_status_col].apply(is_in_process)
]
num_in_process = len(in_process_records)
unique_in_process_sns = len(set(in_process_records[sn_col].unique()))

print(f"Dispositions in process: {num_in_process} rows, {unique_in_process_sns} unique SN")

# Count waiting for material
print("\nCounting waiting for material...")
def is_waiting_for_material(status_text):
    """Check if status indicates waiting for material"""
    if pd.isna(status_text):
        return False
    status_str = str(status_text).lower()
    material_keywords = ['waiting for material', 'strata', 'cx9']
    return any(keyword in status_str for keyword in material_keywords)

# Check both IGS Status and IGS Action for material keywords
waiting_material_records = in_process_records[
    in_process_records[igs_status_col].apply(is_waiting_for_material) |
    in_process_records[igs_action_col].apply(is_waiting_for_material)
]

num_waiting_material = len(waiting_material_records)
unique_waiting_material_sns = len(set(waiting_material_records[sn_col].unique()))

print(f"Waiting for material: {num_waiting_material} rows, {unique_waiting_material_sns} unique SN")

# Calculate BP Duration from completed dispositions
print("\nCalculating average duration of dispositions...")
# Get all completed records: all valid records MINUS Fail records with empty IGS Action
fail_empty_action_sns = set(fail_with_empty_action[sn_col].unique())

# Completed records = all valid records minus Fail records with empty IGS Action
completed_records = valid_sn_records[
    ~(valid_sn_records[sn_col].isin(fail_empty_action_sns) & 
      (valid_sn_records[pic_col].str.upper() == 'IGS') & 
      (valid_sn_records[result_col].str.upper() == 'FAIL'))
]

bp_durations = []
if bp_duration_col in completed_records.columns:
    durations = completed_records[bp_duration_col].dropna().tolist()
    bp_durations = []
    for d in durations:
        try:
            dur = float(d)
            if dur >= 0:
                bp_durations.append(dur)
        except:
            pass

avg_duration = np.mean(bp_durations) if bp_durations else 0
median_duration = np.median(bp_durations) if bp_durations else 0
min_duration = np.min(bp_durations) if bp_durations else 0
max_duration = np.max(bp_durations) if bp_durations else 0
std_duration = np.std(bp_durations) if bp_durations else 0

print(f"Records with BP Duration (from completed dispositions): {len(bp_durations)}")
print(f"Average duration of disposition: {avg_duration:.2f} days")

# Generate HTML report
print("\nGenerating HTML report...")

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>VR-TS1 Bonepile Statistics Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: white; padding: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 10px; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric-box {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
        .metric-box.success {{ background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }}
        .metric-box.warning {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }}
        .metric-box.info {{ background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }}
        .metric-value {{ font-size: 32px; font-weight: bold; margin: 10px 0; }}
        .metric-label {{ font-size: 14px; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #3498db; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f5f5f5; }}
        .summary {{ background: #e8f5e9; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        .note-box {{ background: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #ffc107; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 VR-TS1 BONEPILE STATISTICS REPORT</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        <p><strong>Source:</strong> NV_IGS_VR144_Bonepile.xlsx - Sheet VR-TS1</p>
        
        <div class="note-box">
            <strong>📋 Statistics Logic:</strong><br>
            • Valid SN: Format 183XXXXXXXX (13 digits, starts with 183)<br>
            • Fail: PIC = IGS and result = FAIL<br>
            • Pass: Total unique SN - Fail unique<br>
            • Disposition: Each row with valid SN = 1 disposition (count duplicates)<br>
            • Completed disposition: Total disposition - SN Fail with empty IGS Action<br>
            • In process: IGS Status contains "waiting", "testing", "in process", "in progress"<br>
            • Waiting for material: Contains "waiting for material", "strata", "cx9"
        </div>
        
        <h2>1. TRAY SUMMARY</h2>
        <div class="metric-grid">
            <div class="metric-box">
                <div class="metric-label">Total Trays (Unique SN)</div>
                <div class="metric-value">{total_unique_sns}</div>
            </div>
            <div class="metric-box warning">
                <div class="metric-label">Tray Fail (Unique)</div>
                <div class="metric-value">{total_unique_fail}</div>
            </div>
            <div class="metric-box success">
                <div class="metric-label">Tray Pass (Unique)</div>
                <div class="metric-value">{total_unique_pass}</div>
            </div>
        </div>
        
        <h2>2. DISPOSITION STATISTICS</h2>
        <div class="metric-grid">
            <div class="metric-box info">
                <div class="metric-label">Total Dispositions</div>
                <div class="metric-value">{total_dispositions}</div>
                <div class="metric-label" style="font-size: 11px; margin-top: 5px;">(Each row with valid SN = 1)</div>
            </div>
            <div class="metric-box success">
                <div class="metric-label">Completed Dispositions</div>
                <div class="metric-value">{dispositions_completed}</div>
            </div>
            <div class="metric-box warning">
                <div class="metric-label">SN Fail with Empty IGS Action</div>
                <div class="metric-value">{num_fail_empty_action}</div>
            </div>
        </div>
        
        <h2>3. IN-PROCESS DISPOSITIONS</h2>
        <div class="metric-grid">
            <div class="metric-box info">
                <div class="metric-label">In Process Dispositions</div>
                <div class="metric-value">{num_in_process}</div>
                <div class="metric-label" style="font-size: 11px; margin-top: 5px;">({unique_in_process_sns} unique SN)</div>
            </div>
            <div class="metric-box warning">
                <div class="metric-label">Waiting for Material</div>
                <div class="metric-value">{num_waiting_material}</div>
                <div class="metric-label" style="font-size: 11px; margin-top: 5px;">({unique_waiting_material_sns} unique SN)</div>
            </div>
        </div>
        
        <h2>4. AVERAGE DURATION</h2>
        <div class="summary">
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Records with BP Duration (from completed dispositions)</td><td>{len(bp_durations)}</td></tr>
                <tr><td>Average</td><td>{avg_duration:.2f} days</td></tr>
                <tr><td>Median</td><td>{median_duration:.2f} days</td></tr>
                <tr><td>Min</td><td>{min_duration:.2f} days</td></tr>
                <tr><td>Max</td><td>{max_duration:.2f} days</td></tr>
                <tr><td>Standard Deviation</td><td>{std_duration:.2f} days</td></tr>
            </table>
        </div>
        
        <h2>5. DETAILED STATISTICS</h2>
        <table>
            <tr>
                <th>Item</th>
                <th>Value</th>
                <th>Note</th>
            </tr>
            <tr>
                <td>Total rows with valid SN</td>
                <td><strong>{len(valid_sn_records)}</strong></td>
                <td>Each row = 1 disposition</td>
            </tr>
            <tr>
                <td>Total unique valid SN</td>
                <td><strong>{total_unique_sns}</strong></td>
                <td>Total trays (Fail + Pass)</td>
            </tr>
            <tr>
                <td>Fail rows (PIC=IGS, result=FAIL)</td>
                <td><strong>{total_fail}</strong></td>
                <td>Count duplicates</td>
            </tr>
            <tr>
                <td>Unique SN Fail</td>
                <td><strong>{total_unique_fail}</strong></td>
                <td>No duplicates</td>
            </tr>
            <tr>
                <td>Unique SN Pass</td>
                <td><strong>{total_unique_pass}</strong></td>
                <td>Total unique - Fail unique</td>
            </tr>
            <tr>
                <td>SN Fail with empty IGS Action</td>
                <td><strong>{num_fail_empty_action}</strong></td>
                <td>Not completed</td>
            </tr>
            <tr>
                <td>Completed Dispositions</td>
                <td><strong>{dispositions_completed}</strong></td>
                <td>Total disposition - SN Fail empty action</td>
            </tr>
            <tr>
                <td>In Process Dispositions</td>
                <td><strong>{num_in_process}</strong></td>
                <td>IGS Status: waiting/testing/in process/in progress</td>
            </tr>
            <tr>
                <td>Waiting for Material</td>
                <td><strong>{num_waiting_material}</strong></td>
                <td>Contains: waiting for material/strata/cx9</td>
            </tr>
        </table>
        
        <hr>
        <p style="text-align: center; color: #7f8c8d; margin-top: 50px;">
            Report generated automatically from sheet VR-TS1<br>
            NV_IGS_VR144_Bonepile.xlsx
        </p>
    </div>
</body>
</html>
"""

# Save HTML file
output_file = 'BaoCao_VR_TS1.html'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"\n✓ HTML report generated: {output_file}")
print("\nSummary:")
print(f"  - Total unique valid SN (trays): {total_unique_sns}")
print(f"  - Tray Fail (unique): {total_unique_fail}")
print(f"  - Tray Pass (unique): {total_unique_pass}")
print(f"  - Total Dispositions: {total_dispositions}")
print(f"  - Completed Dispositions: {dispositions_completed}")
print(f"  - SN Fail with empty IGS Action: {num_fail_empty_action}")
print(f"  - In Process Dispositions: {num_in_process} rows ({unique_in_process_sns} unique SN)")
print(f"  - Waiting for Material: {num_waiting_material} rows ({unique_waiting_material_sns} unique SN)")
if bp_durations:
    print(f"  - Average duration: {avg_duration:.2f} days")
print("\nCompleted!")

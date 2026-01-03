#!/usr/bin/env python3
"""
Daily Test Analysis - Phân tích chi tiết test results theo station
Usage: python daily_test_analysis.py <YYYY-MM-DD>
Example: python daily_test_analysis.py 2025-12-30

Phân tích tất cả test results trong ngày:
- Tổng số tray test
- SN nào Fail ở station nào
- SN nào Pass ở station nào
- Thống kê Fail/Pass theo từng station
"""

import sys
import os
import re
import glob
from datetime import datetime
from collections import defaultdict


def parse_filename(filename):
    """
    Parse filename để extract: SN, Status (F/P), Station
    Pattern: IGSJ_PB-6306_675-24109-0000-TS1_1835225000016_F_RIN_20251230T161507Z
            IGSJ_PB-6306_675-24109-0000-TS1_1835225000067_P_RIN_20251230T220011Z
    
    Returns: (sn, status, station) hoặc None nếu không parse được
    """
    # Remove .zip extension
    name = filename.replace('.zip', '')
    
    # Pattern chính: _SN_Status_Station_ (ví dụ: _1835225000016_F_RIN_)
    # SN có thể là 13 digits (18xxxxxxxxxxx) hoặc các format khác
    # Tìm pattern: _SN_Status_Station_ với SN là số có ít nhất 10 digits
    pattern1 = r'_(\d{10,})_([FP])_([A-Z0-9]+)_'
    match1 = re.search(pattern1, name)
    if match1:
        sn = match1.group(1)
        status = match1.group(2)  # F hoặc P
        station = match1.group(3)  # RIN, FLA, etc.
        # Validate SN: phải bắt đầu bằng 18 và có 13 digits
        if sn.startswith('18') and len(sn) == 13:
            return (sn, status, station)
    
    # Pattern 2: Tìm SN 18xxxxxxxxxxx ở bất kỳ đâu, sau đó tìm _Status_Station_
    # Tìm SN pattern: 18 + 11 digits = 13 digits total
    sn_match = re.search(r'(18\d{11})', name)
    if sn_match:
        sn = sn_match.group(1)
        # Tìm pattern _F_Station_ hoặc _P_Station_ gần SN nhất
        # Tìm pattern sau SN
        after_sn = name[name.find(sn) + len(sn):]
        pattern2 = r'_([FP])_([A-Z0-9]+)_'
        match2 = re.search(pattern2, after_sn)
        if match2:
            status = match2.group(1)
            station = match2.group(2)
            return (sn, status, station)
    
    return None


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <YYYY-MM-DD>")
        print(f"Example: {sys.argv[0]} 2025-12-30")
        sys.exit(1)

    # Parse date
    date_str = sys.argv[1]
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        print("Error: Date must be in YYYY-MM-DD format.")
        sys.exit(1)

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        YEAR = date_obj.strftime("%Y")
        MONTH = date_obj.strftime("%m")
        DAY = date_obj.strftime("%d")
    except ValueError:
        print("Error: Invalid date format.")
        sys.exit(1)

    print(f"Analyzing test results for {YEAR}-{MONTH}-{DAY}")
    print("=" * 70)

    # Windows network path
    base_path = r"\\10.16.137.111\Oberon\L10"
    DIR1 = os.path.join(base_path, YEAR, MONTH, DAY)

    if not os.path.isdir(DIR1):
        print(f"Error: Directory {DIR1} does not exist.")
        sys.exit(1)

    # Find all zip files
    zip_files = glob.glob(os.path.join(DIR1, "**", "*.zip"), recursive=True)
    
    if not zip_files:
        print("No zip files found for the specified date.")
        sys.exit(0)

    # Data structures để lưu thông tin
    all_sns = set()  # Tất cả unique SNs (tổng số tray)
    sn_fail_info = defaultdict(list)  # {sn: [(station1, filename1), ...]}
    sn_pass_info = defaultdict(list)  # {sn: [(station1, filename1), ...]}
    sn_pass_rin = set()  # SNs đã PASS ở RIN (chỉ tính những SN này là PASS)
    station_stats = defaultdict(lambda: {'pass': 0, 'fail': 0})  # {station: {'pass': X, 'fail': Y}}
    station_fail_sns = defaultdict(set)  # {station: {sn1, sn2, ...}}
    station_pass_sns = defaultdict(set)  # {station: {sn1, sn2, ...}}

    # Parse tất cả files
    print(f"\nProcessing {len(zip_files)} files...")
    
    for file_path in zip_files:
        filename = os.path.basename(file_path)
        parsed = parse_filename(filename)
        
        if parsed:
            sn, status, station = parsed
            all_sns.add(sn)
            
            if status == 'F':  # Fail
                sn_fail_info[sn].append((station, filename))
                station_stats[station]['fail'] += 1
                station_fail_sns[station].add(sn)
            elif status == 'P':  # Pass
                sn_pass_info[sn].append((station, filename))
                station_stats[station]['pass'] += 1
                station_pass_sns[station].add(sn)
                
                # Chỉ tính PASS khi đã PASS ở RIN
                if station == 'RIN':
                    sn_pass_rin.add(sn)

    # === Tổng kết ===
    total_trays = len(all_sns)
    total_pass = len(sn_pass_rin)  # Chỉ tính SNs đã PASS ở RIN
    total_fail = total_trays - total_pass  # Các SNs còn lại (không PASS ở RIN) = FAIL
    
    print(f"\n{'=' * 70}")
    print("TỔNG KẾT")
    print(f"{'=' * 70}")
    print(f"Tổng số tray test trong ngày: {total_trays}")
    print(f"Tổng số tray PASS (đã PASS ở RIN): {total_pass}")
    print(f"Tổng số tray FAIL (chưa PASS ở RIN): {total_fail}")
    if total_trays > 0:
        pass_rate = (total_pass / total_trays) * 100
        print(f"Pass rate: {pass_rate:.2f}%")
    
    # === Thống kê theo Station ===
    print(f"\n{'=' * 70}")
    print("THỐNG KÊ THEO STATION")
    print(f"{'=' * 70}")
    
    if station_stats:
        # Sort stations alphabetically
        sorted_stations = sorted(station_stats.keys())
        
        for station in sorted_stations:
            stats = station_stats[station]
            fail_count = stats['fail']
            pass_count = stats['pass']
            total_station = fail_count + pass_count
            
            print(f"\nStation: {station}")
            print(f"  - Total tests: {total_station}")
            print(f"  - PASS: {pass_count}")
            print(f"  - FAIL: {fail_count}")
            
            if total_station > 0:
                fail_rate = (fail_count / total_station) * 100
                print(f"  - Fail rate: {fail_rate:.2f}%")
            
            # Unique SNs
            unique_fail_sns = len(station_fail_sns[station])
            unique_pass_sns = len(station_pass_sns[station])
            print(f"  - Unique SNs FAIL: {unique_fail_sns}")
            print(f"  - Unique SNs PASS: {unique_pass_sns}")
    else:
        print("No station data found.")
    
    # === Danh sách SNs FAIL ===
    print(f"\n{'=' * 70}")
    print("DANH SÁCH SERIAL NUMBERS FAIL")
    print(f"{'=' * 70}")
    
    if sn_fail_info:
        sorted_fail_sns = sorted(sn_fail_info.keys())
        for sn in sorted_fail_sns:
            fail_list = sn_fail_info[sn]
            stations = [item[0] for item in fail_list]
            unique_stations = sorted(set(stations))
            print(f"SN {sn}: FAIL tại {', '.join(unique_stations)}")
    else:
        print("Không có SN nào FAIL.")
    
    # === Danh sách SNs PASS (đã PASS ở RIN) ===
    print(f"\n{'=' * 70}")
    print("DANH SÁCH SERIAL NUMBERS PASS (đã PASS ở RIN)")
    print(f"{'=' * 70}")
    
    if sn_pass_rin:
        sorted_pass_rin = sorted(sn_pass_rin)
        for sn in sorted_pass_rin:
            # Lấy tất cả stations mà SN này đã pass
            if sn in sn_pass_info:
                pass_list = sn_pass_info[sn]
                stations = [item[0] for item in pass_list]
                unique_stations = sorted(set(stations))
                print(f"SN {sn}: PASS tại {', '.join(unique_stations)} (đã PASS RIN)")
            else:
                print(f"SN {sn}: PASS tại RIN")
    else:
        print("Không có SN nào PASS ở RIN.")
    
    # === Chi tiết Fail theo Station ===
    print(f"\n{'=' * 70}")
    print("CHI TIẾT FAIL THEO STATION")
    print(f"{'=' * 70}")
    
    if station_fail_sns:
        for station in sorted(station_fail_sns.keys()):
            fail_sns = sorted(station_fail_sns[station])
            print(f"\n{station} - FAIL ({len(fail_sns)} SNs):")
            # In theo nhóm 10 SNs mỗi dòng
            for i in range(0, len(fail_sns), 10):
                print(f"  {', '.join(fail_sns[i:i+10])}")
    else:
        print("Không có Fail nào.")
    
    print(f"\n{'=' * 70}")
    print("Hoàn thành phân tích!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()


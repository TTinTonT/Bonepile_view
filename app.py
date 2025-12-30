#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Flask web application for VR-TS1 Bonepile Statistics
"""

from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import socket

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# Create upload folder if not exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_local_ip():
    """Get local IP address"""
    try:
        # Connect to a remote address to determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

# Function to check valid SN
def is_valid_sn(sn):
    if pd.isna(sn):
        return False
    if isinstance(sn, (int, float)):
        sn_str = str(int(sn))
    else:
        sn_str = str(sn).strip().replace('.0', '')
    return sn_str.startswith('183') and len(sn_str) == 13 and sn_str.isdigit()

# Function to check if status is in process
def is_in_process(status):
    if pd.isna(status):
        return False
    status_str = str(status).lower()
    in_process_keywords = ['waiting', 'testing', 'in process', 'in progress']
    return any(keyword in status_str for keyword in in_process_keywords)

# Function to check waiting for material
def is_waiting_for_material(text):
    if pd.isna(text):
        return False
    text_str = str(text).lower()
    material_keywords = ['waiting for material', 'strata', 'cx9']
    return any(keyword in text_str for keyword in material_keywords)

# Load data
def load_data(filename=None):
    if filename is None:
        # Check if there's an uploaded file
        upload_folder = app.config['UPLOAD_FOLDER']
        excel_files = [f for f in os.listdir(upload_folder) if f.endswith(('.xlsx', '.xls'))]
        if excel_files:
            # Use the most recent file
            excel_file = os.path.join(upload_folder, sorted(excel_files)[-1])
        else:
            # Default file
            excel_file = 'NV_IGS_VR144_Bonepile.xlsx'
            if not os.path.exists(excel_file):
                return None
    else:
        excel_file = filename
    
    if not os.path.exists(excel_file):
        return None
    
    try:
        df = pd.read_excel(excel_file, sheet_name='VR-TS1', header=1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None
    
    sn_col = 'sn'
    pic_col = 'PIC'
    result_col = 'result'
    igs_action_col = 'IGS Action '
    igs_status_col = 'IGS Status'
    bp_duration_col = 'bp_duration'
    nv_disposition_col = 'NV Disposition'
    
    # Remove duplicate header
    if len(df) > 0 and sn_col in df.columns:
        if str(df.iloc[0][sn_col]).strip() == 'sn':
            df = df.iloc[1:].reset_index(drop=True)
    
    # Filter valid SN
    valid_sn_records = df[df[sn_col].apply(is_valid_sn)].copy()
    
    # Get unique SNs
    unique_sns = valid_sn_records[sn_col].unique()
    
    # Fail records
    fail_records = valid_sn_records[
        (valid_sn_records[pic_col].str.upper() == 'IGS') & 
        (valid_sn_records[result_col].str.upper() == 'FAIL')
    ].copy()
    
    unique_fail_sns = set(fail_records[sn_col].unique())
    unique_pass_sns = set(unique_sns) - unique_fail_sns
    
    # Fail with empty IGS Action
    fail_with_empty_action = fail_records[
        (fail_records[igs_action_col].isna()) | 
        (fail_records[igs_action_col].astype(str).str.strip() == '') |
        (fail_records[igs_action_col].astype(str).str.strip() == 'nan')
    ]
    
    # In process dispositions
    fail_with_action = fail_records[~fail_records[sn_col].isin(set(fail_with_empty_action[sn_col].unique()))]
    in_process_records = fail_with_action[
        fail_with_action[igs_status_col].apply(is_in_process)
    ]
    
    # Waiting for material
    waiting_material_records = in_process_records[
        in_process_records[igs_status_col].apply(is_waiting_for_material) |
        in_process_records[igs_action_col].apply(is_waiting_for_material)
    ]
    
    return {
        'df': valid_sn_records,
        'unique_sns': unique_sns,
        'unique_fail_sns': unique_fail_sns,
        'unique_pass_sns': unique_pass_sns,
        'fail_records': fail_records,
        'fail_with_empty_action': fail_with_empty_action,
        'in_process_records': in_process_records,
        'waiting_material_records': waiting_material_records,
        'cols': {
            'sn': sn_col,
            'nv_disposition': nv_disposition_col,
            'igs_action': igs_action_col,
            'igs_status': igs_status_col,
            'bp_duration': bp_duration_col
        }
    }

@app.route('/')
def index():
    try:
        data = load_data()
        if data is None:
            return render_template('index.html', stats=None, error="No Excel file found. Please upload a file.", ip=get_local_ip())
        
        # Calculate statistics
        fail_empty_sns = set(data['fail_with_empty_action'][data['cols']['sn']].unique()) if len(data['fail_with_empty_action']) > 0 else set()
        
        stats = {
            'total_trays': len(data['unique_sns']),
            'total_fail': len(data['unique_fail_sns']),
            'total_pass': len(data['unique_pass_sns']),
            'total_dispositions': len(data['df']),
            'completed_dispositions': len(data['df']) - len(fail_empty_sns),
            'fail_empty_action': len(fail_empty_sns),
            'in_process': len(data['in_process_records']),
            'in_process_unique': len(set(data['in_process_records'][data['cols']['sn']].unique())) if len(data['in_process_records']) > 0 else 0,
            'waiting_material': len(data['waiting_material_records']),
            'waiting_material_unique': len(set(data['waiting_material_records'][data['cols']['sn']].unique())) if len(data['waiting_material_records']) > 0 else 0
        }
    
        # Calculate average duration
        fail_empty_action_sns = fail_empty_sns
        completed_records = data['df'][
            ~(data['df'][data['cols']['sn']].isin(fail_empty_action_sns) & 
              (data['df']['PIC'].str.upper() == 'IGS') & 
              (data['df']['result'].str.upper() == 'FAIL'))
        ]
        
        bp_durations = []
        if data['cols']['bp_duration'] in completed_records.columns:
            for d in completed_records[data['cols']['bp_duration']].dropna():
                try:
                    dur = float(d)
                    if dur >= 0:
                        bp_durations.append(dur)
                except:
                    pass
        
        stats['avg_duration'] = float(np.mean(bp_durations)) if bp_durations else 0.0
        stats['median_duration'] = float(np.median(bp_durations)) if bp_durations else 0.0
        stats['min_duration'] = float(np.min(bp_durations)) if bp_durations else 0.0
        stats['max_duration'] = float(np.max(bp_durations)) if bp_durations else 0.0
        stats['std_duration'] = float(np.std(bp_durations)) if bp_durations else 0.0
        stats['duration_count'] = len(bp_durations)
        
        return render_template('index.html', stats=stats, error=None, ip=get_local_ip())
    except Exception as e:
        return render_template('index.html', stats=None, error=f"Error loading data: {str(e)}", ip=get_local_ip())

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # Save with a fixed name so we always use the latest upload
            filename = 'NV_IGS_VR144_Bonepile.xlsx'
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            flash('File uploaded successfully!')
            return redirect(url_for('index'))
        else:
            flash('Invalid file type. Please upload .xlsx or .xls file.')
            return redirect(request.url)
    
    return render_template('upload.html', ip=get_local_ip())

@app.route('/api/sn-list/<category>')
def get_sn_list(category):
    try:
        data = load_data()
        if data is None:
            return jsonify({'error': 'No data available'}), 404
        
        if category == 'total':
            sns = sorted([str(int(sn)) if isinstance(sn, (int, float)) else str(sn) 
                         for sn in data['unique_sns']])
        elif category == 'fail':
            sns = sorted([str(int(sn)) if isinstance(sn, (int, float)) else str(sn) 
                         for sn in data['unique_fail_sns']])
        elif category == 'pass':
            sns = sorted([str(int(sn)) if isinstance(sn, (int, float)) else str(sn) 
                         for sn in data['unique_pass_sns']])
        else:
            sns = []
        
        return jsonify({'sns': sns, 'count': len(sns)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/fail-empty-action')
def get_fail_empty_action():
    try:
        data = load_data()
        if data is None:
            return jsonify({'error': 'No data available'}), 404
        
        result = []
        for idx, row in data['fail_with_empty_action'].iterrows():
            sn = row[data['cols']['sn']]
            sn_str = str(int(sn)) if isinstance(sn, (int, float)) else str(sn)
            nv_disp = row[data['cols']['nv_disposition']] if pd.notna(row[data['cols']['nv_disposition']]) else ''
            
            result.append({
                'sn': sn_str,
                'nv_disposition': str(nv_disp)
            })
        
        return jsonify({'data': result, 'count': len(result)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/in-process')
def get_in_process():
    try:
        data = load_data()
        if data is None:
            return jsonify({'error': 'No data available'}), 404
        
        result = []
        for idx, row in data['in_process_records'].iterrows():
            sn = row[data['cols']['sn']]
            sn_str = str(int(sn)) if isinstance(sn, (int, float)) else str(sn)
            nv_disp = row[data['cols']['nv_disposition']] if pd.notna(row[data['cols']['nv_disposition']]) else ''
            igs_action = row[data['cols']['igs_action']] if pd.notna(row[data['cols']['igs_action']]) else ''
            
            result.append({
                'sn': sn_str,
                'nv_disposition': str(nv_disp),
                'igs_action': str(igs_action)
            })
        
        return jsonify({'data': result, 'count': len(result)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/waiting-material')
def get_waiting_material():
    try:
        data = load_data()
        if data is None:
            return jsonify({'error': 'No data available'}), 404
        
        result = []
        for idx, row in data['waiting_material_records'].iterrows():
            sn = row[data['cols']['sn']]
            sn_str = str(int(sn)) if isinstance(sn, (int, float)) else str(sn)
            nv_disp = row[data['cols']['nv_disposition']] if pd.notna(row[data['cols']['nv_disposition']]) else ''
            igs_action = row[data['cols']['igs_action']] if pd.notna(row[data['cols']['igs_action']]) else ''
            igs_status = row[data['cols']['igs_status']] if pd.notna(row[data['cols']['igs_status']]) else ''
            
            result.append({
                'sn': sn_str,
                'nv_disposition': str(nv_disp),
                'igs_action': str(igs_action),
                'igs_status': str(igs_status)
            })
        
        return jsonify({'data': result, 'count': len(result)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    local_ip = get_local_ip()
    print("=" * 80)
    print("VR-TS1 Bonepile Statistics Dashboard")
    print("=" * 80)
    print(f"Starting server...")
    print(f"Local access: http://localhost:5000")
    print(f"Network access: http://{local_ip}:5000")
    print("=" * 80)
    app.run(debug=True, host='0.0.0.0', port=5000)


# VR-TS Bonepile Statistics Dashboard

Web-based dashboard for VR-TS Bonepile statistics (supports multiple VR-TS* sheets) with interactive charts and detailed views.

## Features

- **Interactive Statistics**: Click on metric boxes to view detailed SN lists
- **Charts**: Pie chart for tray distribution, bar chart for disposition statistics
- **Detailed Views**:
  - Total/Fail/Pass tray SN lists
  - Fail with empty IGS Action (shows NV Disposition)
  - In Process dispositions (shows NV Disposition + IGS Action)
  - Waiting for Material (shows NV Disposition + IGS Action + IGS Status)

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

1. Run the Flask application:
```bash
python app.py
```

2. The application will display the server IP address when it starts. You can access it via:
   - **Local access**: http://localhost:5000
   - **Network access**: http://<your-pc-ip>:5000 (shown when starting the app)

3. Upload Excel File:
   - Click "Upload New File" button on the dashboard
   - Or navigate to http://<your-pc-ip>:5000/upload
   - Upload your Excel file (.xlsx or .xls)
   - The file will be analyzed and statistics will be displayed

4. **Note**: Anyone on the same WiFi network can access the dashboard using your PC's IP address.

## API Endpoints

- `GET /` - Main dashboard page
- `GET /api/sn-list/<category>` - Get SN list (category: total, fail, pass)
- `GET /api/fail-empty-action` - Get fail records with empty IGS Action
- `GET /api/in-process` - Get in-process dispositions
- `GET /api/waiting-material` - Get waiting for material records

## Statistics Logic

- **Valid SN**: Format 183XXXXXXXX (13 digits, starts with 183)
- **Fail**: PIC = IGS and result = FAIL
- **Pass**: Total unique SN - Fail unique
- **Disposition**: Each row with valid SN = 1 disposition (count duplicates)
- **Completed disposition**: Total disposition - SN Fail with empty IGS Action
- **In process**: IGS Status contains "waiting", "testing", "in process", "in progress"
- **Waiting for material**: Contains "waiting for material", "strata", "cx9"

## Deployment

For production deployment, use a WSGI server like Gunicorn:

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```


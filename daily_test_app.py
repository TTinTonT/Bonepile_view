#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Daily Test Analysis - Development Server (Port 5005)
This is a separate Flask app instance for Daily Test Analysis development.
Once completed, routes will be merged back to main app.py on port 5001.
"""

# Import everything from app.py to reuse all functions and routes
from app import app, get_local_ip
import app as app_module

# Use context processor to inject show_daily_test_button=True for all templates
@app.context_processor
def inject_daily_test_button():
    """Inject show_daily_test_button=True into all templates"""
    return dict(show_daily_test_button=True)

if __name__ == '__main__':
    local_ip = get_local_ip()
    port = 5005
    #print("=" * 80)
    ##print("Daily Test Analysis - Development Server")
    #print("=" * 80)
    #print(f"Starting server on port {port}...")
    #print(f"Local access: http://localhost:{port}")
   # print(f"Network access: http://{local_ip}:{port}")
   # print(f"Daily Test Analysis: http://localhost:{port}/daily-test-analysis")
    #print("=" * 80)
    #print("NOTE: This is a development server. Routes will be merged to main app (port 5001) when completed.")
    #print("=" * 80)
    app.run(debug=True, host='0.0.0.0', port=port)

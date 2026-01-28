@echo off
cd /d "%~dp0"
echo ========================================
echo Daily Test Analysis Server
echo ========================================
echo.
echo Starting server...
echo.
py daily_test_app.py
pause


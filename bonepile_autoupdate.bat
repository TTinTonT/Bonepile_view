@echo off
setlocal

REM Runs update_and_restart.ps1 every 5 minutes.
REM Cancel:
REM  - close this window, OR
REM  - Task Manager -> end cmd.exe/powershell.exe, OR
REM  - remove this .bat from Startup folder.

cd /d "%~dp0"

:loop
powershell -ExecutionPolicy Bypass -NoProfile -WindowStyle Hidden -File "%~dp0update_and_restart.ps1"
timeout /t 300 /nobreak >nul
goto loop


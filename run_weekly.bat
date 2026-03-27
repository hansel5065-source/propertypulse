@echo off
echo ============================================
echo  PropertyPulse — Weekly Lead Update
echo ============================================
echo.

echo [1/3] Scraping PropWire (all 4 counties)...
echo.
echo NOTE: Chrome must be running with remote debugging.
echo       If not, run this first:
echo       "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
echo       Then log into propwire.com and re-run this script.
echo.
python scrape_propwire_leads.py
if %errorlevel% neq 0 (
    echo [!] PropWire scrape failed — continuing without it
)

echo.
echo [2/3] Running pipeline (merge Apify + PropWire, score, dedupe)...
python pipeline.py --append
if %errorlevel% neq 0 (
    echo [!] Pipeline failed
    pause
    exit /b 1
)

echo.
echo [3/3] Done! Open PropertyPulse to see new leads.
echo.
pause

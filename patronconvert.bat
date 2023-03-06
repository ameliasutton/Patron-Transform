CLS
ECHO OFF

:: Runs Patron Import Prep Script
ECHO Starting Patron Import Prep Script:
python convertPatronData.py
ECHO Patron Import Files Prepared!
PAUSE

:: Finds Most Recent Patron Load File
for /f %%i in ('dir FOLIO_Patron_Convert_Output\Patron_load /b/a-d/od/t:c') do set LAST=%%i
ECHO Last Patron File: %LAST%
SET PATRONS=%LAST%
PAUSE

CLS
ECHO OFF

:: Runs Patron Import Prep Script
ECHO Starting Patron Import Prep Script:
python convertPatronData.py
ECHO Patron Import Files Prepared!
PAUSE

:: Finds Most Recent Student File
for /f %%i in ('dir FOLIO_Patron_Convert_Output\Student_load /b/a-d/od/t:c') do set LAST=%%i
ECHO Last Student: %LAST%
SET STUDENT=%LAST%
PAUSE
:: Finds Most Recent Staff File
for /f %%i in ('dir FOLIO_Patron_Convert_Output\Staff_load /b/a-d/od/t:c') do set LAST=%%i
ECHO Last Staff: %LAST%
SET STAFF=%LAST%
PAUSE
@echo off
title DB Restore - DedupFS
set BUNDLEDJAVA="%~dp0jre\bin\java.exe"
if exist %BUNDLEDJAVA% ( set JAVA=%BUNDLEDJAVA% ) else ( set JAVA=java )
for /f tokens^=2-3^ delims^=.^" %%j in ('%JAVA% -fullversion 2^>^&1') do set JAVAVERSION=%%j.%%k
if not "%JAVAVERSION%"=="11.0" (
  echo "This software has been tested with Java 11.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)

echo.
echo This utility will restore a previous state of the DedupFS database.
echo This will overwrite the current database state.  If you do not want
echo that, simply close the console window where this utility is running.
echo.
pause
echo.


rem ### Parameters ###
rem # repo=<repository directory>     | default: '..' (parent of working directory)
rem # from=<restore script file name> | default: none - for simple db file restore
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx512m -cp "%~dp0lib\*" dedup.dbRestore %*
if not errorlevel 0 (
    echo Database restore finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database restore finished.
    pause
)

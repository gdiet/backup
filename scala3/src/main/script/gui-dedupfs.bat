@echo off
title DedupFS
set BUNDLEDJAVA="%~dp0jre\bin\javaw.exe"
if exist %BUNDLEDJAVA% ( set JAVA=%BUNDLEDJAVA% ) else ( set JAVA=javaw )
for /f tokens^=2-3^ delims^=.^" %%j in ('%JAVA% -fullversion 2^>^&1') do set JAVAVERSION=%%j.%%k
if not "%JAVAVERSION%"=="11.0" (
  echo "This software has been tested with Java 11.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)

rem ### Parameters ###
rem # repo=<repository directory> | default: '..' (parent of working directory)
rem # mount=<mount point>         | default: J:\
rem # readOnly=true               | default: false
rem # noDbBackup=true             | default: false
rem # copyWhenMoving=true         | default: false
rem # gui=true                    | default: false, true in script - true to show server GUI
rem # temp=<temp dir>             | default: 'dedupfs-temp' in the user's temp dir
echo Starting the DedupFS server GUI now - it will run detached from the command line.
start "DedupFS" %JAVA% "-DLOG_BASE=%~dp0log" -Xmx512m -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.mount gui=true %*

@echo off
title DedupFS
set BUNDLEDJAVA="%~dp0jre\bin\java"
if exist %BUNDLEDJAVA% ( set JAVA=BUNDLEDJAVA ) else ( set JAVA=java )
for /f tokens^=2-3^ delims^=.^" %%j in ('%JAVA% -fullversion 2^>^&1') do set JAVAVERSION=%%j.%%k
if not "%JAVAVERSION%"=="11.0" (
  echo "This software has been tested with Java 11.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)

rem ### Parameters ###
rem # repo=<repository directory>     | optional, default: working directory
rem # mountPoint=<mount point>        | mandatory, e.g. J:\ or /mnt/dedupfs
rem # readOnly=true                   | optional, default: false
rem # noDbBackup=true                 | optional, default: false
rem # copyWhenMoving=true             | optional, default: false
rem # temp=<temp dir>                 | optional, default: 'dedupfs-temp' in the user's temp dir
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx512m -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.mount %*
pause

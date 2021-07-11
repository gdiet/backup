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

rem ### Mount options ###
rem # Log directory: -DLOG_BASE=<log base directory>
rem # Heap space   : -Xmx256M - mount needs at least 96MB RAM.
rem # 1. parameter : <repository directory>
rem # 2. parameter : <mount point>
rem # Additional optional parameters:
rem # -readOnly=true
rem # -noDbBackup=true
rem # -copyWhenMoving=true
rem # -temp=<temp dir>
rem
rem ### Note ###
rem # -Dfile.encoding=UTF-8 is necessary for FUSE operations.
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx512m -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.mount %*
pause

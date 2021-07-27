@echo off
title Reclaim Space 1 - DedupFS
set BUNDLEDJAVA="%~dp0jre\bin\java.exe"
if exist %BUNDLEDJAVA% ( set JAVA=%BUNDLEDJAVA% ) else ( set JAVA=java )
for /f tokens^=2-3^ delims^=.^" %%j in ('%JAVA% -fullversion 2^>^&1') do set JAVAVERSION=%%j.%%k
if not "%JAVAVERSION%"=="11.0" (
  echo "This software has been tested with Java 11.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)

rem ### Parameters ###
rem # repo=<repository directory>       | default: '..' (parent of working directory)
%JAVA% "-DLOG_BASE=%~dp0log" -cp "%~dp0lib\*" dedup.reclaimSpace2 %*
pause

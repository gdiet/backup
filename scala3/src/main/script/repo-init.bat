@echo off
title Repo Init DedupFS
rem Init options:
rem First parameter: <repository directory>
rem Log directory: -DLOG_BASE=<log base directory>
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\log" -cp "%~dp0lib\*" dedup.init %*
pause

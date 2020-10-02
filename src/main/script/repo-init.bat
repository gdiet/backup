@echo off
rem Options for init:
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server init %*
pause

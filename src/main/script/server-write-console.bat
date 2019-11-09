@echo off
if exist fsdb\dedupfs.trace.db (
  echo Database trace file fsdb\dedupfs.trace.db found.
  echo Check for database problems first.
  echo.
  pause
  exit /B
)
call %~dp0dbbackup.bat
rem Options: init write repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
rem Server needs at least ~400MB of memory.
%~dp0jre\bin\java -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server write temp=%TEMP%

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
rem Possible main classes:
rem * dedup.Server (console, use "java")
rem * dedup.TrayApp (use "javaw")
rem * dedup.ServerGui (use "javaw")
rem Server needs at least ~400MB of memory.
start "DedupFS" %~dp0jre\bin\javaw -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.ServerGui write temp=%TEMP%

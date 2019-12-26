@echo off
set dbfile=fsdb\dedupfs.mv.db
if "%time:~0,1%" == " " (set hour=0%time:~1,1%) else (set hour=%time:~0,2%)
set timestamp=%date:~6,4%-%date:~3,2%-%date:~0,2%_%hour%-%time:~3,2%
set target=fsdb/dedupfs_%timestamp%.zip
rem All 'set' commands must be outside of the 'if', else they are not evaluated as needed.
if exist %dbfile% (
  echo "Creating plain database backup: %dbfile% -> %dbfile%.backup"
  copy %dbfile% %dbfile%.backup
  echo "Creating sql script database backup: %dbfile% -> %target%"
  jre\bin\java.exe -cp lib\* org.h2.tools.Script -url "jdbc:h2:%~dp0fsdb\dedupfs" -script %target% -user sa -options compression zip
  attrib +R %target%
  pause
) else (
  echo "Database backup: Database file %dbfile% does not exist."
  pause
)

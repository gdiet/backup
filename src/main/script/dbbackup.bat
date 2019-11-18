@echo off
if "%time:~0,1%" == " " (set hour=0%time:~1,1%) else (set hour=%time:~0,2%)
set timestamp=%date:~6,4%-%date:~3,2%-%date:~0,2%_%hour%-%time:~3,2%
if exist "fsdb\dedupfs.mv.db" (
  echo "Starting database backup."
  echo Creating sql script database backup...
  echo fsdb\dedupfs.mv.db -^> fsdb\dedupfs_%timestamp%.zip
  jre\bin\java.exe -cp lib\* org.h2.tools.Script -url "jdbc:h2:%~dp0fsdb\dedupfs" -script fsdb\dedupfs_%timestamp%.zip -user sa -options compression zip
  attrib +R fsdb\dedupfs_%timestamp%.zip
) else (
  echo "Database backup: Database file fsdb\dedupfs.mv.db not found."
  pause
)

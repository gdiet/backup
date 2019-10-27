@echo off
if "%time:~0,1%" == " " (set timestamp=0%time:~1,1%) else (set timestamp=%time:~0,2%)
set timestamp=%date:~6,4%-%date:~3,2%-%date:~0,2%_%timestamp%-%time:~3,2%
echo Creating sql script database backup...
echo fsdb\dedupfs.mv.db -^> fsdb\dedupfs_%timestamp%.zip
jre\bin\java.exe -cp lib\* org.h2.tools.Script -url "jdbc:h2:%~dp0fsdb\dedupfs" -script fsdb\dedupfs_%timestamp%.zip -user sa -options compression zip
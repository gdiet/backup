@echo off
title DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory> | default: '..' (parent of working directory)
rem # mount=<mount point>         | default: J:\ - the 'mount=' prefix can be omitted
rem # readOnly=true               | default: false, true in script
rem # noDbBackup=true             | default: false
rem # copyWhenMoving=true         | default: false
rem # gui=true                    | default: false - true to show server GUI
rem # temp=<temp dir>             | default: 'dedupfs-temp' in the user's temp dir
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx80m -Dfile.encoding=UTF-8 -Dlogback.configurationFile=logback-server.xml -cp "%~dp0lib\*" dedup.mount readOnly=true %*
if errorlevel 0 if not errorlevel 1 (
    pause
) else (
    echo Dedupfs process finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)

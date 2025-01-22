@echo off
title File System Command - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory> | default: '..' (parent of working directory)
rem ### Commands ###
rem # call 'fsc help' for more information
set readonly=false
IF "%1" == "find" set readonly=true
IF "%1" == "list" set readonly=true
IF "%1" == "restore" set readonly=true
IF "%1" == "stats" set readonly=true
IF "%readonly%" == "false" (
  %JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.fsc %*
) ELSE (
  %JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -Dlogback.configurationFile=logback-readonly.xml -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.fsc %*
)
rem # No pause, no additional output, fsc is a plain command line tool.

@echo off
title File System Command - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory> | default: '..' (parent of working directory)
rem ### Commands ###
rem # backup <source> <target> [options] | store files in the backup repository
rem # db-backup                          | create a database backup
rem # db-restore [backup-zip]            | restore a database backup
rem # db-compact                         | compact the database file
rem # find <pattern>   | find files with matching file names, e.g. '*.scala'
rem # list <path>      | list directory or show file info
rem # del <path>       | mark a file or recursively mark a directory as deleted
set readonly=false
IF "%1" == "find" set readonly=true
IF "%1" == "list" set readonly=true
IF "%readonly%" == "false" (
  %JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*" dedup.fsc %*
) ELSE (
  %JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -Dlogback.configurationFile=logback-readonly.xml -cp "%~dp0lib\*" dedup.fsc %*
)
rem # No pause, no additional output, fsc is a plain command line tool.

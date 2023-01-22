@echo off
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%


rem ### Parameters ###
rem # repo=<repository directory>     | default: '..' (parent of working directory)
rem ### Commands ###
rem # backup            | create a database backup
rem # find <matcher>    | find files with matching file names, e.g. '%.scala'
rem # list <path>       | list directory or show file info
rem # del  <path>       | mark a file or recursively mark a directory as deleted
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*" dedup.fsc %*

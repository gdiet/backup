@echo off
title Statistics - DedupFS
call %~dp0helpers\set-java.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>     | default: '..' (parent of working directory)
rem ### Commands ###
rem # find <matcher>    | find files with matching file names, e.g. '%.scala'
rem # list <path>       | list directory or show file info
rem # del <path>        | mark a file as deleted
rem # rmdir <path>      | recursively mark a directory as deleted
%JAVA% "-DLOG_BASE=%~dp0log" -cp "%~dp0lib\*" dedup.fsc %*

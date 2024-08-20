@echo off
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
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*" dedup.fsc %*

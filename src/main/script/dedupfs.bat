@echo off
call %~dp0db-backup.bat %*

if not errorlevel 1 (
    title Write DedupFS
    rem Options for write:
    rem copywhenmoving=true             | default: false
    rem mount=K:\                       | default: Windows J:\ Linux /tmp/mnt
    rem repo=<repository directory>     | default: working directory
    rem temp=<temp file directory>      | default: <system temp dir>/dedupfs-temp
    rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
    rem -Dfile.encoding=UTF-8           | necessary for FUSE operations
    rem write needs at least 96MB RAM.
    "%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server write %*
)

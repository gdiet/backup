@rem Options: init write repo=repositoryDirectory mount=mountPoint
@call %~dp0dbbackup.bat
@start "DedupFS" %~dp0jre\bin\javaw -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.TrayApp write

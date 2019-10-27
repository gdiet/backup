@rem Options: init write repo=repositoryDirectory mount=mountPoint
@call %~dp0dbbackup.bat
@%~dp0jre\bin\java -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server write

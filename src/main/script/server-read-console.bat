@rem Options: init write repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
@%~dp0jre\bin\java -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server
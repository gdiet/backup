@rem Options: init write repo=repositoryDirectory mount=mountPoint
@%~dp0openjdk-11u-11.0.4_11-jre\bin\java -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server write

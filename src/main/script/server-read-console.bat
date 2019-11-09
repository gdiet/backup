@rem Options: init write repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
@rem Server needs at least ~400MB of memory.
@%~dp0jre\bin\java -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server

@rem Options: init write copyWhenMoving repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
@rem Server needs at least ~400MB of memory.
@start "DedupFS" %~dp0jre\bin\javaw -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.TrayApp

@rem Options: init write repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
@rem Possible main classes:
@rem * dedup.Server (console, use "java")
@rem * dedup.TrayApp (use "javaw")
@rem * dedup.ServerGui (use "javaw")
@rem Server needs at least ~400MB of memory.
@start "DedupFS" %~dp0jre\bin\javaw -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.ServerGui

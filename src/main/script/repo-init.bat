@"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.Server init %*
@pause

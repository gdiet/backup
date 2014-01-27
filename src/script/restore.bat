if "%JAVA_HOME%"=="" (
	set JAVA_EXE=java
) else (
	set JAVA_EXE=%JAVA_HOME%\bin\java.exe
)

"%JAVA_EXE%" -Xmx256m -cp "%~dp0lib/*" net.diet_rich.dedup.restore.Restore %*
pause
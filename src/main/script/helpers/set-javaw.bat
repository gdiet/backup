set BUNDLEDJAVAW="%~dp0..\jre\bin\javaw.exe"
if exist %BUNDLEDJAVAW% ( set JAVAW=%BUNDLEDJAVAW% ) else ( set JAVAW=javaw )
for /f tokens^=2^ delims^=.+^" %%j in ('%JAVAW% -fullversion 2^>^&1') do set JAVAVERSION=%%j
if not "%JAVAVERSION%"=="21" (
  echo "This software has been tested with Java 21 only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)
exit /B 0

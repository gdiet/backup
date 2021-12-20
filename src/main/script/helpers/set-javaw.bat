set BUNDLEDJAVAW="%~dp0..\jre\bin\javaw.exe"
if exist %BUNDLEDJAVAW% ( set JAVAW=%BUNDLEDJAVAW% ) else ( set JAVAW=javaw )
for /f tokens^=2-3^ delims^=.^" %%j in ('%JAVAW% -fullversion 2^>^&1') do set JAVAVERSION=%%j.%%k
if not "%JAVAVERSION%"=="17.0" (
  echo "This software has been tested with Java 17.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)

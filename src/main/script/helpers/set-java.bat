set BUNDLEDJAVA="%~dp0..\jre\bin\java.exe"
if exist %BUNDLEDJAVA% ( set JAVA=%BUNDLEDJAVA% ) else ( set JAVA=java )
for /f tokens^=2^ delims^=.+^" %%j in ('%JAVA% -fullversion 2^>^&1') do set JAVAVERSION=%%j
if not "%JAVAVERSION%"=="21" (
  echo "This software has been tested with Java 17.0.* only."
  echo "Detected Java version: %JAVAVERSION%.*"
  pause
  exit /B 1
)
exit /B 0

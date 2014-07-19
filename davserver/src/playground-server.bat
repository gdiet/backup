@echo off
pushd %~dp0

set RUNINLINE=true

if "%JAVA_HOME%"=="" (
	echo.
	echo JAVA_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

cd ..\..
call sbt.bat davserver/xitrumPackage

if "%RUNINLINE%"=="" (
	start "davserver" "%JAVA_HOME%\bin\java.exe" -cp target/xitrum/lib/* net.diet_rich.dedup.webdav.ServerApp ../target/playRepo READWRITE
) else (
	"%JAVA_HOME%\bin\java.exe" -cp target/xitrum/lib/* net.diet_rich.dedup.webdav.ServerApp ../target/playRepo READWRITE
)

popd

@echo off
cd %~dp0

if "%SBT_HOME%"=="" (
	echo.
	echo SBT_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

cd ..
call "%SBT_HOME%\bin\sbt.bat" ;xitrumPackage ;davserver/xitrumPackage
cd davserver
start "davserver" java -cp target/xitrum/lib/* net.diet_rich.dedup.webdav.ServerApp

@echo off
cd %~dp0

if "%SBT_HOME%"=="" (
	echo.
	echo SBT_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

if "%JAVA_HOME%"=="" (
	echo.
	echo JAVA_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

call "%SBT_HOME%\bin\sbt.bat" xitrumPackage

set repo=target\playRepo

set classpath=target/xitrum/lib/*

"%JAVA_HOME%\bin\java.exe" -cp %classpath% net.diet_rich.dedup.repository.Fix -g n -r %repo% -o updateDatabase

pause
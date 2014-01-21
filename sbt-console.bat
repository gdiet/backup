
@echo off
cd %~dp0

if "%SBT_HOME%"=="" (
	echo.
	echo SBT_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

call "%SBT_HOME%\bin\sbt.bat"
@echo off
cd %~dp0

if "%SBT_HOME%"=="" (
	echo.
	echo SBT_HOME is not set, exiting...
	echo.
	pause
	exit /b -1
)

call "%SBT_HOME%\bin\sbt.bat" xitrumPackage

set repo=target\playRepo

rd /S /Q %repo%
md %repo%

set classpath=target/xitrum/lib/*

java -cp %classpath% net.diet_rich.dedup.repository.Create -r %repo% -g n
java -cp %classpath% net.diet_rich.dedup.backup.Backup -r %repo% -g n -s src -t /src -i n

pause
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

cd ..
call "%SBT_HOME%\bin\sbt.bat" ;xitrumPackage ;ftpserver/xitrumPackage
cd ftpserver

if "%RUNINLINE%"=="" (
	start "ftpserver" "%JAVA_HOME%\bin\java.exe" -cp target/xitrum/lib/* net.diet_rich.dedup.ftpserver.FtpServer %*
) else (
	"%JAVA_HOME%\bin\java.exe" -cp target/xitrum/lib/* net.diet_rich.dedup.ftpserver.FtpServer %*
)

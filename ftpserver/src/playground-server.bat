@echo off
pushd %~dp0

if "%JAVA_HOME%"=="" (
	echo JAVA_HOME is not set, exiting...
	pause
	exit /b -1
)

set RUNINLINE=true
set REPODIR=./target/playRepo
set CLASSPATH=ftpserver/target/xitrum/lib/*
set JAVACALL="%JAVA_HOME%\bin\java.exe" -cp %CLASSPATH%
set CREATEREPOCALL= %JAVACALL% net.diet_rich.dedup.core.CreateRepository %REPODIR%
set FTPSERVERCALL= %JAVACALL% net.diet_rich.dedup.ftpserver.FtpServerApp %REPODIR% READWRITE

cd ..\..
call sbt.bat core/package ;ftpserver/xitrumPackage

if not exist target/playRepo (
    echo *** creating repository ...
    mkdir target\playRepo
    %CREATEREPOCALL%
)

echo *** starting ftp server ...
if "%RUNINLINE%"=="" (
	start "davserver" %FTPSERVERCALL%
) else (
	%FTPSERVERCALL%
)

popd

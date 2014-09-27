@echo off
pushd %~dp0

if "%JAVA_HOME%"=="" (
	echo JAVA_HOME is not set, exiting...
	pause
	exit /b -1
)

set RUNINLINE=true
set REPODIR=./target/playRepo
set CLASSPATH=davserver/target/xitrum/lib/*;davserver/src/main/resources
set JAVACALL="%JAVA_HOME%\bin\java.exe" -cp %CLASSPATH%
set CREATEREPOCALL= %JAVACALL% net.diet_rich.dedup.core.CreateRepositoryApp %REPODIR%
set DAVSERVERCALL= %JAVACALL% net.diet_rich.dedup.webdav.DavServerApp %REPODIR% READWRITE

cd ..\..
call sbt.bat core/package ;davserver/xitrumPackage

if not exist target/playRepo (
    echo *** creating repository ...
    mkdir target\playRepo
    %CREATEREPOCALL%
)

echo *** starting webdav server ...
if "%RUNINLINE%"=="" (
	start "davserver" %DAVSERVERCALL%
) else (
	%DAVSERVERCALL%
)

popd

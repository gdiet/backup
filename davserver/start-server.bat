@echo off
cd %~dp0
cd ..
call e:\georg\bin\sbt\bin\sbt.bat ;xitrumPackage ;davserver/xitrumPackage
cd davserver
start "davserver" java -cp target/xitrum/lib/* net.diet_rich.dedup.webdav.ServerApp

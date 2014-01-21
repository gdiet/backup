cd %~dp0

call e:\georg\bin\sbt\bin\sbt.bat xitrumPackage

set repo=target\playRepo

rd /S /Q %repo%
md %repo%

set classpath=target/xitrum/lib/*

java -cp %classpath% net.diet_rich.dedup.repository.Create -r %repo% -g n
java -cp %classpath% net.diet_rich.dedup.backup.Backup -r %repo% -g n -s src -t /src -i n

pause
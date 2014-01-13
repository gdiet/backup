cd %~dp0

set repo=target\playRepo

rd /S /Q %repo%
md %repo%

set classpath=lib_managed/jars/org.scala-lang/scala-library/*;lib_managed/jars/com.h2database/h2/*;target/scala-2.10/dedup_2.10-0.03-SNAPSHOT.jar 

java -cp %classpath% net.diet_rich.dedup.repository.Create -r %repo% -g n
java -cp %classpath% net.diet_rich.dedup.backup.Backup -r %repo% -g n -s src -t /src -i n

pause
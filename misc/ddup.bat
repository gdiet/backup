@set timestamp=%DATE:~6,4%.%DATE:~3,2%.%DATE:~0,2%_%TIME:~0,2%.%TIME:~3,2%
@set timestamp=%timestamp: =0%
@set Path=%~dp0openjdk-11u-11.0.4_11-jre\bin
@%~dp0app\dedup.bat repo=%~dp0 mount=J:\ target=%timestamp% %*

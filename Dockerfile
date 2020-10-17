FROM hseeberger/scala-sbt:11.0.8_1.4.0_2.13.3

# Linux & Windows JRE for app
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10/OpenJDK11U-jre_x64_linux_hotspot_11.0.8_10.tar.gz
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10/OpenJDK11U-jre_x64_windows_hotspot_11.0.8_10.zip

# Prepare SBT and download dependencies
COPY build.sbt .
COPY project/build.properties project/
RUN sbt update

# Build. Note that the JARs contain the resource files.
COPY src src
RUN sbt createApp

# Final touches
COPY *.version target/app/
COPY readme.html target/app/

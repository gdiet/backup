FROM hseeberger/scala-sbt:11.0.8_1.4.0_2.13.3

# Linux & Windows JRE for app
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10/OpenJDK11U-jre_x64_linux_hotspot_11.0.8_10.tar.gz
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10/OpenJDK11U-jre_x64_windows_hotspot_11.0.8_10.zip

# Install jq, needed for generating html readme
RUN apt-get --assume-yes install jq

# Prepare SBT and download dependencies
COPY build.sbt .
COPY project/build.properties project/
RUN sbt update

# Build. Note that the JARs contain the resource files.
COPY src src
RUN sbt createApp

# unpack JREs
RUN tar xfz OpenJDK11U-jre_x64_linux_hotspot_11.0.8_10.tar.gz
RUN mv jdk-*-jre target/app/jrex
RUN unzip -q OpenJDK11U-jre_x64_windows_hotspot_11.0.8_10.zip
RUN mv jdk-*-jre target/app/jre

# Copy license and version file
COPY LICENSE target/app/
COPY *.version target/app/

# Create HTML documentation
# Thanks to https://github.com/jfroche/docker-markdown and to the GitHub api
COPY README.md .
RUN jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md \
      | curl -s --data @- https://api.github.com/markdown > target/app/readme.html  || exit 1

# Final touches
RUN chmod -x target/app/*.bat
RUN chmod -x target/app/*.html
RUN chmod -x target/app/*.legal
RUN chmod -x target/app/*.version

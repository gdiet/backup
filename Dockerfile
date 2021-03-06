# https://github.com/hseeberger/scala-sbt
FROM hseeberger/scala-sbt:11.0.9.1_1.4.5_2.13.4

# Install additional utilities needed later
RUN apt-get --assume-yes install jq zip

# Prepare SBT and download dependencies
COPY build.sbt .
COPY project/build.properties project/
RUN sbt update

# Build. Note that the JARs contain the resource files.
COPY src src
RUN sbt createApp

# unpack JREs
COPY OpenJDK11U-jre_x64_* ./
RUN tar xfz OpenJDK11U-jre_x64_linux_hotspot_11.0.10_9.tar.gz
RUN mv jdk-*-jre target/app/jrex
RUN unzip -q OpenJDK11U-jre_x64_windows_hotspot_11.0.10_9.zip
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
WORKDIR /root/target/app
RUN chmod +x *
RUN chmod -x *.bat
RUN chmod -x *.html
RUN chmod -x *.legal
RUN chmod -x *.version

# Pack apps
RUN zip -rq ../../app.zip .
RUN tar cfz ../../app.tar.gz *

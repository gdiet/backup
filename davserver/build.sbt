// add milton webdav (see http://milton.io).
// milton uses slf4j as logging frontend. as logging backend, use logback
// (see http://www.slf4j.org/manual.html and http://logback.qos.ch).

resolvers +=
  "ettrema-repo" at "http://milton.io/maven"

libraryDependencies ++= Seq (
  "io.milton" % "milton-api" % "2.5.2.5",
  "io.milton" % "milton-server-ce" % "2.5.2.5",
  "commons-lang" % "commons-lang" % "2.6", // needed but regrettably not included automatically
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "ch.qos.logback" % "logback-classic" % "1.0.13"
)


// add jetty (see http://www.eclipse.org/jetty).

libraryDependencies ++= Seq (
  "org.eclipse.jetty" % "jetty-server" % "9.1.1.v20140108",
  // TODO still needed?
  "org.eclipse.jetty" % "jetty-servlet" % "9.1.1.v20140108"
)


// other settings

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.withSource := true

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

retrieveManaged := true
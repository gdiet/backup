resolvers += "ettrema-repo" at "http://milton.io/maven"

// Add milton server (see http://milton.io)
libraryDependencies ++= Seq (
  "io.milton" % "milton-api" % "2.6.5.0",
  "io.milton" % "milton-server-ce" % "2.6.5.0",
  "commons-lang" % "commons-lang" % "2.6" // Needed but not included automatically
)

// Add jetty server (see http://www.eclipse.org/jetty)
libraryDependencies ++= Seq (
  "org.eclipse.jetty" % "jetty-server" % "9.3.0.M2",
  "org.eclipse.jetty" % "jetty-servlet" % "9.3.0.M2"
)

XitrumPackage.copy()

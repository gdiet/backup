libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.4.186",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.specs2" %% "specs2-core" % "2.4.17" % "test"
)

XitrumPackage.copy()

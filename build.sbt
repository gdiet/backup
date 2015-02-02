// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version in ThisBuild := "0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.5"

fork in ThisBuild := true

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps,implicitConversions"
)

XitrumPackage.copy()

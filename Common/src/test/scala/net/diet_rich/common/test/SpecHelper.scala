package net.diet_rich.common.test

import java.io.File

import net.diet_rich.common.io._

trait SpecHelper {
  def className = getClass.getSimpleName
  def testDataDirectory = new File(System.getProperty("java.io.tmpdir")) / "_specs2_" / className
}

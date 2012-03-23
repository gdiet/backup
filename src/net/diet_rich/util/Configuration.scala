package net.diet_rich.util

import collection.mutable.{Map => MutableMap}
import java.io.File
import io.using
import scala.io.Source

// TODO make a read-only configuration
class Configuration(val map: MutableMap[String, String], val file: Option[File]) {
  def string(key: String) : String = map(key)
  def long(key: String) : Long = string(key).toLong
  def set(key: String, value: Any) : Option[String] = map.put(key, value toString)
}

object Configuration {
  def apply(file: File, defaults: Map[String, String] = Map()) : Configuration = {
    val values = MutableMap[String, String]() ++= defaults
    if (file isFile()) {
      val valueLines = using(Source fromFile (file, "UTF-8"))(_ getLines)
        .map(_ trim)
        .filterNot(_ isEmpty)
        .filterNot(_ startsWith("#"))
      values ++= valueLines.map{_ split("=")}.map(elem => (elem(0), elem(1)))
    }
    new Configuration(values, Some(file))
  }
  
  def apply(entries: Map[String, String]) : Configuration =
    new Configuration(MutableMap[String, String]() ++= entries, None)
}

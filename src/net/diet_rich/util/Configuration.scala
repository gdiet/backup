package net.diet_rich.util

import Configuration._

class Configuration (val map: StringMap) {
  def string(key: String) : String = map get key get
  def long(key: String) : Long = string(key) toLong
  def int(key: String) : Int = string(key) toInt
  def hasTheseKeys(keys: String*) : Boolean =
    (map.keySet -- keys).isEmpty && (keys.toSet -- map.keySet).isEmpty
  def writeConfigFile(file: java.io.File) =
    io.using(new java.io.PrintWriter(file, "UTF-8")){ writer =>
      map foreach { case (key, value) => writer println "%s=%s".format(key, value) }
    }
}

object Configuration {
  type StringMap = Map[String, String]
  
  implicit def extendMapWithConfiguration(map: StringMap) = new Configuration(map)
  
  def readConfigFile(file: java.io.File) : StringMap = {
    io.using(scala.io.Source fromFile (file, "UTF-8"))(
        _.getLines
        .map(_ trim)
        .filterNot(_ isEmpty)
        .filterNot(_ startsWith("#"))
        .map{_ split("=")}
        .map( elem => (elem(0),elem(1)) )
        .toMap
    )
  }
}

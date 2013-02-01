// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import io.readSettingsFile

trait CmdApp { import CmdApp._
  protected val usageHeader: String
  protected val paramData: Seq[((String, String), String)]
  protected def application(options: Map[String, String]): Unit
  
  private lazy val usage: String = try {
    val paramDataForUsage = paramData :+
      (CONFIGFILESWITCH -> "" -> "[%s <file path>] Optional: Configuration file")
    val lines = (usageHeader + "Parameters:") +: paramDataForUsage.map{ case ((k, v), msg) => msg.format(k, v) }
    lines.mkString("\n")
  } catch { case e: Throwable => "Oops ... error while building usage string!" }
  
  private def collectOptions(argMap: Map[String, String]): Map[String, String] = {
    val defaults = paramData.map(_._1).toMap
    val configFileSettings = argMap.get(CONFIGFILESWITCH).map { fileName =>
      val file = new java.io.File(fileName)
      require(file.isFile, s"Configuration file path $file does not denote a file.")
      readSettingsFile(file)
    }.getOrElse(Map())
    val result = defaults ++ configFileSettings ++ argMap
    require(result.keySet == defaults.keySet, s"Unexpected parameter(s): ${(result.keySet -- defaults.keySet).mkString("'", "', '", "'")}")
    result
  }
  
  def run(args: Array[String]): Boolean =
    try {
      run(argsToMap(args))
      true
    } catch {
      case e: Throwable =>
        println(s"$usage\n\n$e\n${e.getStackTraceString}")
        if (e.getCause() != null) println(s"caused by ${e.getCause().getStackTraceString}")
        false
    }
    
  def run(argMap: Map[String, String]): Unit = {
    val options = collectOptions(argMap)
    application(options)
  }
}

object CmdApp {
  val CONFIGFILESWITCH = "-c"

  def argsToMap(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, s"args must be key/value pairs (number of args found: ${args.length})")
    args.sliding(2, 2).map(e => e(0) -> e(1)).toMap
  }
}

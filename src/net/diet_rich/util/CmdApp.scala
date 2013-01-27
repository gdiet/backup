// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import io.readSettingsFile

trait CmdApp { import CmdApp._
  val usageHeader: String
  val paramData: Seq[((String, String), String)]
  
  lazy val usage: String = try {
    val paramDataForUsage = paramData :+
      (CONFIGFILESWITCH -> "" -> "[%s <file path>] Optional: Configuration file")
    val lines = (usageHeader + "Parameters:") +: paramDataForUsage.map{ case ((k, v), msg) => msg.format(k, v) }
    lines.mkString("\n")
  } catch { case e: Throwable => "Oops ... error while building usage string!" }
    
  def run(args: Array[String])(code: Map[String, String] => Unit): Boolean = {
    try {
      val argMap = Args.toMap(args)
      val defaults = paramData.map(_._1).toMap
      val configFileSettings = argMap.get(CONFIGFILESWITCH).map { fileName =>
        val file = new java.io.File(fileName)
        require(file.isFile, s"Configuration file path $file does not denote a file.")
        readSettingsFile(file)
      }.getOrElse(Map())
      val opts = defaults ++ configFileSettings ++ argMap
      require(opts.keySet == defaults.keySet, s"Unexpected parameter(s): ${(opts.keySet -- defaults.keySet).mkString(" / ")}")
      code(opts)
      true
    } catch {
      case e: Throwable =>
        println(s"$usage\n\n$e\n${e.getStackTraceString}")
        if (e.getCause() != null) println(s"caused by ${e.getCause().getStackTraceString}")
        false
    }
  }
}
object CmdApp {
  val CONFIGFILESWITCH = "-c"
}

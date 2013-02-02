// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import io.readSettingsFile

trait CmdApp { import CmdApp._
  private type KeysAndHints = Seq[((String, String), String)]
  
  protected val usageHeader: String
  protected val keysAndHints: KeysAndHints
  protected val optionalKeysAndHints: KeysAndHints = Seq()
  protected def application(options: Map[String, String]): Unit

  private lazy val optionalKeys = optionalKeysAndHints.map(_._1._1)
  private val localKeysDefaultsAndHints = Seq(
    CONFIGFILESWITCH -> "" -> "[%s <file path>] Optional: Configuration file"
  )

  private def formatHintsForUsage(input: KeysAndHints): Seq[String] =
    input.map{ case ((key, default), hint) => hint.format(key, default) }
  
  private lazy val usage: String = try {
    val lines =
      (usageHeader +: "" +: "Mandatory parameters:" +:
      formatHintsForUsage(keysAndHints)) ++
      ("" +: "Optional parameters:" +:
      formatHintsForUsage(optionalKeysAndHints ++ localKeysDefaultsAndHints))
    lines.mkString("\n")
  } catch { case e: Throwable => usageHeader + "\n\nOops ... error while building usage string!" }
  
  private def collectOptions(argMap: Map[String, String]): Map[String, String] = {
    val defaults = (keysAndHints ++ optionalKeysAndHints).map(_._1).toMap
    val configFileSettings = argMap.get(CONFIGFILESWITCH).map { fileName =>
      val file = new java.io.File(fileName)
      require(file.isFile, s"Configuration file path $file does not denote a file.")
      readSettingsFile(file)
    }.getOrElse(Map())
    val result = defaults ++ configFileSettings ++ argMap
    require(result.keySet - CONFIGFILESWITCH == defaults.keySet, s"Unexpected parameter(s): ${(result.keySet -- defaults.keySet).mkString("'", "', '", "'")}")
    val emptyParameter = (result -- optionalKeys).find(_._2.isEmpty)
    require(emptyParameter.isEmpty, s"Parameter ${emptyParameter.get._1} is mandatory, but is empty.")
    result
  }
  
  def run(args: Array[String]): Boolean =
    try {
      run(argsToMap(args))
      true
    } catch {
      case e: Throwable =>
        println(s"$usage\n")
        e.printStackTrace(System.out)
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

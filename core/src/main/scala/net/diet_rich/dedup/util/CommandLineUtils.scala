package net.diet_rich.dedup.util

import java.io.File

import net.diet_rich.dedup.core.data.StoreMethod

object CommandLineUtils {
  def forArgs(args: Array[String])(body: ArgDetails => Unit) =
    if (args.length < 2) println("Arguments: <Command> <Repository> [key:value settings]") else {
      val command :: repoPath :: options = args.toList
      val optionsMap = options
        .map { option => option.splitAt(option.indexOf(':')) }
        .map { case (key, value) => (key, value.tail) }
        .toMap
      body(new ArgDetails(command, new File(repoPath), optionsMap))
    }
}

class ArgDetails(val command: String, val repositoryDir: File, private val options: Map[String, String]) extends Logging {
  log info s"Executing command $command with repository $repositoryDir"

  private var remainingOptions = options

  def opt(key: String, prefix: String): Option[String] = init(options get key) { option =>
    log info s"$prefix $key: ${option getOrElse "not set"}"
    remainingOptions = remainingOptions - key
  }
  def optional(key: String): Option[String] = opt(key, "Optional setting")
  def required(key: String): String = opt(key, "Mandatory setting") getOrElse (throw new IllegalArgumentException(s"Setting '$key' is mandatory"))
  def intOptional(key: String) = optional(key) map (_ toInt)
  def booleanOptional(key: String) = optional(key) map (_ toBoolean)

  def checkOptionUse(options: Any*) = if (remainingOptions nonEmpty) {
    remainingOptions foreach { case (key, value) =>
      log warn s"Option $key: $value is not evaluated in command $command"
    }
    throw new IllegalArgumentException(s"${remainingOptions.size} settings are not needed for command $command")
  }

  lazy val checkPrint       = booleanOptional("checkPrint")
  lazy val hashAlgorithm    = optional("hashAlgorithm")
  lazy val maxBytesToCache  = intOptional("maxBytesToCache")
  lazy val parallel         = intOptional("parallel")
  lazy val port             = intOptional("port")
  lazy val reference        = optional("reference")
  lazy val repositoryid     = optional("repositoryid")
  lazy val source           = required("source")
  lazy val sourceFile       = new File(source)
  lazy val storeBlockSize   = intOptional("storeBlockSize")
  lazy val storeMethod      = optional("storeMethod") map StoreMethod.named
  lazy val target           = required("target")
  lazy val targetFile       = new File(target)
  lazy val versionComment   = optional("versionComment")
  lazy val writable         = Writable fromBooleanString optional("writable").getOrElse("false")

}

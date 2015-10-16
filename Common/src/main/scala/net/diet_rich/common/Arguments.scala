package net.diet_rich.common

import scala.collection.mutable

class Arguments(args: Array[String], numberOfParams: Int) {
  require(args.length >= numberOfParams, s"$numberOfParams arguments are mandatory, but only ${args.length} arguments are provided")
  private val (paramsList, optionsList) = args.toList.splitAt(numberOfParams)
  private val optionsMap = optionsList
    .map { option => option.splitAt(option.indexOf(':')) }
    .map { case (key, value) => (key, value.tail) }
    .toMap
  private val checkedOptions = mutable.Set[String]()
  private def opt(key: String, prefix: String): Option[String] = {
    checkedOptions add key
    optionsMap get key
  }
  def parameters: List[String] = paramsList
  def optional(key: String): Option[String] = opt(key, "Optional setting")
  def required(key: String): String = opt(key, "Mandatory setting") getOrElse (throw new IllegalArgumentException(s"Setting '$key' is mandatory"))
  def intOptional(key: String) = optional(key) map (_.toInt)
  def longOptional(key: String) = optional(key) map (_.toLong)
  def booleanOptional(key: String) = optional(key) map (_.toBoolean)
  def withSettingsChecked(f: => Unit): Unit = {
    val availableSettings = optionsMap.keySet
    if ((availableSettings -- checkedOptions).isEmpty) f else {
      println(s"The first $numberOfParams arguments are evaluated as parameters.")
      println("The following additional settings provided have not been evaluated:")
      println(s"  ${(availableSettings -- checkedOptions) mkString ", "}")
      println("The following settings are evaluated:")
      println(s"  ${checkedOptions mkString ", "}")
      println("\nExiting...")
    }
  }
}

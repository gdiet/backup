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
  private val remainingOptions = mutable.Set(optionsMap.keys.toSeq:_*)
  private def opt(key: String, prefix: String): Option[String] = {
    checkedOptions add key
    remainingOptions remove key
    optionsMap get key
  }
  def parameters: List[String] = paramsList
  def optional(key: String): Option[String] = opt(key, "Optional setting")
  def required(key: String): String = opt(key, "Mandatory setting") getOrElse (throw new IllegalArgumentException(s"Setting '$key' is mandatory"))
  def intOptional(key: String) = optional(key) map (_.toInt)
  def booleanOptional(key: String) = optional(key) map (_.toBoolean)
}

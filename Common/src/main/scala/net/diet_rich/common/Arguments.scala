package net.diet_rich.common

class Arguments(args: Array[String], numberOfParams: Int) {
  private val (params, options) = args.splitAt(numberOfParams)
  private val optionsMap = options
    .map { option => option.splitAt(option.indexOf(':')) }
    .map { case (key, value) => (key, value.drop(1)) }
    .toMap
  private var uncheckedOptions = optionsMap.keySet
  private var problems = Seq.empty[String]

  if (params.length < numberOfParams) problems :+= s"At least $numberOfParams parameters are required."

  def parameters: List[String] = if (params.length == numberOfParams) params.toList else List.fill(numberOfParams)("")
  def optional(key: String): Option[String] = { uncheckedOptions -= key; optionsMap.get(key) }
  def required(key: String): String = optional(key).getOrElse { problems :+= s"The parameter $key is required."; "" }
  def optionalLong(key: String): Option[Long] =
    try optional(key).map(_.toLong)
    catch { case _: NumberFormatException => problems :+= s"The parameter $key is not a valid number."; None }

  def withSettingsChecked[T](bad: String => T)(good: => T): T =
    if (problems.nonEmpty)
      bad(problems.mkString("\n"))
    else if (uncheckedOptions.nonEmpty)
      bad("The following options provided have not been evaluated:\n" + uncheckedOptions.mkString("'", "', '", "'"))
    else
      try good catch { case t: Throwable =>
        t.printStackTrace(System.err) // FIXME use logger
        bad(s"Execution failed with $t")
      }
}

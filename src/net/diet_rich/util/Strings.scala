// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Strings {
  def normalizeMultiline(string: String): String = {
    val lines = string.lines.toList
    assume (lines.head isEmpty, s"expected empty first line in multiline string <$string>")
    val leadingBlanks = lines.tail.head.indexWhere(_ != ' ')
    assume (leadingBlanks >= 0, s"expected non-blank in second line of multiline string <$string>")
    val reverse = lines.tail.reverse
    assume (reverse.head.forall(_ == ' '), s"expected only blanks in last line <${reverse.head}> of multiline string <$string>")
    reverse.tail.reverse.map(_.substring(leadingBlanks)).mkString("\n")
  }
}

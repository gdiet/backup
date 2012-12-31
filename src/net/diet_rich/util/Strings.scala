// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Strings {
  def normalizeMultiline(string: String): String = {
    val lines = string.lines.toList
    require (lines.head isEmpty, "expected empty first line in multiline string <%s>" format string)
    val leadingBlanks = lines.tail.head.indexWhere(_ != ' ')
    require (leadingBlanks >= 0, "expected non-blank in second line of multiline string <%s>" format string)
    val reverse = lines.tail.reverse
    require (reverse.head.size == leadingBlanks, "expected only blanks in last line of multiline string <%s>" format string)
    reverse.tail.reverse.map(_.substring(leadingBlanks)).mkString("\n")
  }
}

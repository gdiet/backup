// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Strings {
  def normalizeMultiline(string: String): String = {
    val lines = string.lines.toList
    assume (lines.head isEmpty, "expected empty first line in multiline string <%s>" format string)
    val leadingBlanks = lines.tail.head.indexWhere(_ != ' ')
    assume (leadingBlanks >= 0, "expected non-blank in second line of multiline string <%s>" format string)
    val reverse = lines.tail.reverse
    assume (reverse.head.forall(_ == ' '), "expected only blanks in last line <%s> of multiline string <%s>" format (reverse.head, string))
    reverse.tail.reverse.map(_.substring(leadingBlanks)).mkString("\n")
  }
}

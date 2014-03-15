// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {

  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  
  implicit class AugmentedString(val string: String) extends AnyVal {
    def normalizeMultiline: String = {
      val lines = string.lines.toList
      assume (lines.head isEmpty, s"expected empty first line in multiline string <$string>")
      val leadingBlanks = lines.tail.head.indexWhere(_ != ' ')
      assume (leadingBlanks >= 0, s"expected non-blank in second line of multiline string <$string>")
      assume (lines.forall(_.take(leadingBlanks).trim isEmpty), s"expected only blanks in the first $leadingBlanks characters of a line in multiline string <$string>")
      val reverse = lines.tail.reverse
      assume (reverse.head.forall(_ == ' '), s"expected only blanks in last line <${reverse.head}> of multiline string <$string>")
      reverse.tail.reverse.map(_.substring(leadingBlanks)).mkString("\n")
    }
    def processSpecialSyntax(rule1: String=>String, rule2: String=>String): String = {
      string.split('!').sliding(2, 2).map(_.toList).map{
        case List(a,b) => List(rule1(a), rule2(b))
        case List(a) => List(rule1(a))
        case _ => throw new IllegalStateException
      }.flatten.mkString
    }
  }
}
// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {

  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  
  implicit class AugmentedString(val string: String) extends AnyVal {
    def normalizeMultiline = Strings normalizeMultiline string
    def processSpecialSyntax = Strings.processSpecialSyntax(string) _
  }
}
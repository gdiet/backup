// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {
  implicit def normalizeMultilineString(string: String) = new Object {
    def normalizeMultiline = Strings.normalizeMultiline(string)
  }
}

// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {
  implicit class EnhancedString(val value: String) extends AnyVal {
    def normalizeMultiline = Strings.normalizeMultiline(value)
  }
  implicit class EnhancedLong(val value: Long) extends AnyVal {
    def toByteArray = {
      val data = new Array[Byte](8)
      ByteArrayUtil.writeLong(data, 0, value)
      data
    }
  }
}

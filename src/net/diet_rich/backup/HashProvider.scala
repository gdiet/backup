// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.security.MessageDigest
import java.io.InputStream
import java.security.DigestInputStream

object HashProvider {
  def digester(algorithm: String) : MessageDigest =
    MessageDigest.getInstance(algorithm)
}

// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.security.MessageDigest.getInstance

object Hash {
  def checkAlgorithm(algorithm: String): String = {
    getInstance(algorithm)
    algorithm
  }
  def getLength(algorithm: String): Int =
    getInstance(algorithm).getDigestLength
}
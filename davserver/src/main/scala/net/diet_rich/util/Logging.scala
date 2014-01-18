// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import org.slf4j.LoggerFactory

trait Logging {
  protected val log = LoggerFactory getLogger (this getClass)
}

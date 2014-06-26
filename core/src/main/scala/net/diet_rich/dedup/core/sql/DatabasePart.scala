// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import net.diet_rich.dedup.util.ThreadSpecific

trait DatabasePart {
  protected val database: Database
  
  private val sessions = ThreadSpecific(database createSession)
  protected implicit final def session: Session = sessions.threadInstance
}

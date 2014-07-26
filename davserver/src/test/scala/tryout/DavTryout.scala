// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package tryout

import net.diet_rich.dedup.testutil._
import net.diet_rich.dedup.webdav.ServerApp

object DavTryout extends App {

  val repository = testDir("DavRepo")
  ServerApp.main(Array(s"$repository", "READWRITE"))

}

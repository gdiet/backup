// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package tryout

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.testutil._

object DavCreate extends App {

  val repository = newTestDir("DavRepo")
  Repository.create(repository)

}

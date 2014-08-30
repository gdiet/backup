// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.servlet.DefaultMiltonConfigurator

class DedupMiltonConfigurator(resourceFactory: ResourceFactory) extends DefaultMiltonConfigurator {
  override protected def build(): Unit = {
    builder setMainResourceFactory resourceFactory
    super.build()
  }
}

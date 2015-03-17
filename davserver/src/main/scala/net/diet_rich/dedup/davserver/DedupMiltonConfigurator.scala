package net.diet_rich.dedup.davserver

import io.milton.http.ResourceFactory
import io.milton.servlet.DefaultMiltonConfigurator

class DedupMiltonConfigurator(resourceFactory: ResourceFactory) extends DefaultMiltonConfigurator {
  override protected def build(): Unit = {
    builder setMainResourceFactory resourceFactory
    super.build()
  }
}

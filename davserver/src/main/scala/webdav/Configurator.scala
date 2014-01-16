package webdav

import io.milton.servlet.DefaultMiltonConfigurator

class Configurator extends DefaultMiltonConfigurator{

  override def configure(config: io.milton.servlet.Config) = {
    builder.setMainResourceFactory(new com.myastronomy.AstronomyResourceFactory)
    super.configure(config)
  }
  
}
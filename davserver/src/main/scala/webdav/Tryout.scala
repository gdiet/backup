package webdav

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHandler
import io.milton.servlet.MiltonFilter
import javax.servlet.FilterConfig
import org.eclipse.jetty.servlet.FilterHolder
import org.eclipse.jetty.servlet.FilterMapping

object Tryout extends App {
  val filterHolder = new FilterHolder(new MiltonFilter)
  filterHolder.setInitParameter("resource.factory.class", "com.myastronomy.AstronomyResourceFactory")
  
  val servletHandler = new ServletHandler()
  servletHandler.addFilterWithMapping(filterHolder, "/*", FilterMapping.REQUEST);
  
  val server = new Server(8080)
  server.setHandler(servletHandler)
  server.start()
  server.join()
}
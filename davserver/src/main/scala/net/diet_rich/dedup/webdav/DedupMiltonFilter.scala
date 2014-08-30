// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import javax.servlet._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import io.milton.http.HttpManager
import io.milton.servlet.{FilterConfigWrapper, MiltonConfigurator}

class DedupMiltonFilter(configurator: MiltonConfigurator) extends Filter {
  private var context: ServletContext = null
  private var httpManager: HttpManager = null

  override def init(config: FilterConfig): Unit = {
    context = config.getServletContext
    httpManager = configurator configure new FilterConfigWrapper(config)
  }

  override def doFilter(request: ServletRequest, response: ServletResponse, filterChain: FilterChain): Unit = (request, response) match {
    case (httpRequest: HttpServletRequest, httpResponse: HttpServletResponse) =>
      httpManager process (
        new io.milton.servlet.ServletRequest(httpRequest, context),
        new io.milton.servlet.ServletResponse(httpResponse)
      )
    case _ => filterChain.doFilter(request, response)
  }

  override def destroy(): Unit = configurator shutdown()
}
